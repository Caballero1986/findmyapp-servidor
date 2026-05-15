const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const cors       = require('cors');

const app      = express();
const servidor = http.createServer(app);
const io       = new Server(servidor, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
});

app.use(cors());
app.use(express.json());

// ── Estado en memoria ────────────────────────────────────────────────────────
const grupos      = {};
const nombreIndex = {};
const fcmTokens   = {};

function asegurarGrupo(grupo) {
  if (!grupos[grupo])      grupos[grupo]      = {};
  if (!nombreIndex[grupo]) nombreIndex[grupo] = {};
  if (!fcmTokens[grupo])   fcmTokens[grupo]   = {};
}

// ── Firebase Admin ───────────────────────────────────────────────────────────
let adminApp = null;

function inicializarFirebase() {
  try {
    let projectId, clientEmail, privateKey;

    // 1. Archivo local (desarrollo / Railway con archivo subido)
    try {
      const cfg = require('./firebaseConfig.js');
      projectId   = cfg.projectId;
      clientEmail = cfg.clientEmail;
      privateKey  = cfg.privateKey;
      console.log('[FCM] Usando firebaseConfig.js');
    } catch (e) {
      // 2. Variables de entorno (Railway env vars)
      projectId   = process.env.FIREBASE_PROJECT_ID;
      clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
      privateKey  = process.env.FIREBASE_PRIVATE_KEY;
      console.log('[FCM] Usando variables de entorno');
      console.log('[FCM] projectId:', projectId);
      console.log('[FCM] clientEmail:', clientEmail);
      console.log('[FCM] privateKey existe:', !!privateKey);
    }

    if (!projectId || !clientEmail || !privateKey) {
      console.log('[FCM] Sin credenciales — FCM deshabilitado');
      return;
    }

    const admin = require('firebase-admin');
    admin.initializeApp({
      credential: admin.credential.cert({
        projectId,
        clientEmail,
        privateKey: privateKey.replace(/\\n/g, '\n'),
      }),
    });
    adminApp = admin;
    console.log('[FCM] Firebase Admin inicializado OK');
  } catch (e) {
    console.log('[FCM] Error:', e.message);
  }
}

// ── Socket.IO ────────────────────────────────────────────────────────────────
io.on('connection', socket => {
  let miGrupo  = null;
  let miNombre = null;

  socket.on('unirse_grupo', datos => {
    miGrupo  = datos.codigoGrupo;
    miNombre = datos.nombre || 'Usuario';

    socket.join(miGrupo);
    asegurarGrupo(miGrupo);

    if (datos.fcmToken) {
      fcmTokens[miGrupo][miNombre] = datos.fcmToken;
      console.log('Token FCM guardado para:', miNombre);
    }

    const anterior = nombreIndex[miGrupo][miNombre];
    if (anterior && anterior !== socket.id) {
      console.log('Eliminando duplicado de:', miNombre);
      delete grupos[miGrupo][anterior];
      io.to(miGrupo).emit('usuario_desconectado', anterior);
      const socketAnterior = io.sockets.sockets.get(anterior);
      if (socketAnterior) {
        delete nombreIndex[miGrupo][miNombre];
        socketAnterior.disconnect(true);
      }
    }

    nombreIndex[miGrupo][miNombre] = socket.id;
    socket.emit('ubicaciones_iniciales', Object.values(grupos[miGrupo]));
    console.log(miNombre, 'se unió al grupo', miGrupo);
  });

  socket.on('enviar_ubicacion', datos => {
    if (!miGrupo) return;
    asegurarGrupo(miGrupo);
    grupos[miGrupo][socket.id] = {
      lat:       datos.lat,
      lng:       datos.lng,
      nombre:    datos.nombre || miNombre,
      socketId:  socket.id,
      ultimaVez: new Date().toISOString(),
    };
    socket.to(miGrupo).emit('ubicacion_actualizada', grupos[miGrupo][socket.id]);
  });

  socket.on('pedir_actualizacion', async () => {
    if (!miGrupo) return;

    io.to(miGrupo).emit('forzar_actualizacion');

    if (!adminApp) {
      console.log('[FCM] No disponible — solo notificando usuarios conectados');
      return;
    }

    const tokens = Object.values(fcmTokens[miGrupo] || {}).filter(Boolean);
    if (tokens.length === 0) return;

    console.log('[FCM] Enviando push a', tokens.length, 'usuarios');

    for (const token of tokens) {
      try {
        await adminApp.messaging().send({
          token,
          data: { tipo: 'pedir_ubicacion', grupo: miGrupo, origen: miNombre },
          android: { priority: 'high' },
        });
        console.log('[FCM] Push enviado OK');
      } catch (err) {
        console.log('[FCM] Error enviando push:', err.message);
        if (err.code === 'messaging/registration-token-not-registered') {
          const nombre = Object.keys(fcmTokens[miGrupo])
            .find(n => fcmTokens[miGrupo][n] === token);
          if (nombre) delete fcmTokens[miGrupo][nombre];
        }
      }
    }
  });

  socket.on('disconnect', razon => {
    console.log('Desconectado:', socket.id, '| Razón:', razon);
    if (!miGrupo) return;

    if (grupos[miGrupo]) delete grupos[miGrupo][socket.id];

    if (miNombre && nombreIndex[miGrupo] &&
        nombreIndex[miGrupo][miNombre] === socket.id) {
      delete nombreIndex[miGrupo][miNombre];
    }

    if (grupos[miGrupo] && Object.keys(grupos[miGrupo]).length === 0) {
      delete grupos[miGrupo];
      delete nombreIndex[miGrupo];
      delete fcmTokens[miGrupo];
    }

    io.to(miGrupo).emit('usuario_desconectado', socket.id);
  });
});

// ── HTTP endpoints ───────────────────────────────────────────────────────────
app.get('/', (_req, res) => res.json({ estado: 'ok' }));

app.get('/status', (_req, res) =>
  res.json({
    fcm:              adminApp !== null,
    grupos:           Object.keys(grupos).length,
    hora:             new Date().toISOString(),
    vars_firebase:    Object.keys(process.env).filter(k => k.startsWith('FIREBASE')),
    env_project_id:   !!process.env.FIREBASE_PROJECT_ID,
    env_client_email: !!process.env.FIREBASE_CLIENT_EMAIL,
    env_private_key:  !!process.env.FIREBASE_PRIVATE_KEY,
  })
);

// ── Arranque ─────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;

servidor.listen(PORT, '0.0.0.0', () => {
  console.log('Servidor escuchando en puerto ' + PORT);
  inicializarFirebase();
});

process.on('SIGTERM', () => {
  console.log('SIGTERM recibido — cerrando limpiamente');
  servidor.close(() => process.exit(0));
});
