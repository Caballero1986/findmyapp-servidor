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
let messaging = null;

function inicializarFirebase() {
  const projectId   = process.env['FIREBASE_PROJECT_ID'];
  const clientEmail = process.env['FIREBASE_CLIENT_EMAIL'];
  const privateKey  = process.env['FIREBASE_PRIVATE_KEY'];

  console.log('[FCM] projectId:', projectId);
  console.log('[FCM] clientEmail:', clientEmail);
  console.log('[FCM] privateKey existe:', !!privateKey);
  console.log('[FCM] Todas las vars Firebase:', Object.keys(process.env).filter(k => k.startsWith('FIREBASE')));

  let serviceAccount = null;

  // 1. Archivo local — desarrollo
  try {
    const fs   = require('fs');
    const path = require('path');
    const file = path.join(__dirname, 'serviceAccount.json');
    if (fs.existsSync(file)) {
      serviceAccount = JSON.parse(fs.readFileSync(file, 'utf8'));
      console.log('[FCM] Usando serviceAccount.json local');
    }
  } catch (e) {
    console.log('[FCM] Error leyendo serviceAccount.json:', e.message);
  }

  // 2. Variables individuales — Railway
  if (!serviceAccount && projectId) {
    serviceAccount = {
      type:         'service_account',
      project_id:   projectId,
      client_email: clientEmail,
      private_key:  (privateKey || '').replace(/\\n/g, '\n'),
    };
    console.log('[FCM] Usando variables individuales');
  }

  // 3. JSON completo — fallback
  if (!serviceAccount && process.env['FIREBASE_SERVICE_ACCOUNT']) {
    try {
      serviceAccount = JSON.parse(process.env['FIREBASE_SERVICE_ACCOUNT']);
      console.log('[FCM] Usando FIREBASE_SERVICE_ACCOUNT JSON');
    } catch {
      console.log('[FCM] FIREBASE_SERVICE_ACCOUNT no es JSON válido');
    }
  }

  if (!serviceAccount) {
    console.log('[FCM] Sin credenciales — FCM deshabilitado');
    return;
  }

  try {
    const admin = require('firebase-admin');
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    messaging = admin.messaging();
    console.log('[FCM] Firebase Admin inicializado correctamente');
  } catch (e) {
    console.log('[FCM] Error inicializando Firebase Admin:', e.message);
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

    if (!messaging) {
      console.log('[FCM] No disponible — solo notificando usuarios conectados');
      return;
    }

    const tokens = Object.values(fcmTokens[miGrupo] || {}).filter(Boolean);
    if (tokens.length === 0) return;

    console.log('[FCM] Enviando push a', tokens.length, 'usuarios');

    for (const token of tokens) {
      try {
        await messaging.send({
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
    fcm:            messaging !== null,
    grupos:         Object.keys(grupos).length,
    hora:           new Date().toISOString(),
    vars_firebase:  Object.keys(process.env).filter(k => k.startsWith('FIREBASE')),
    env_project_id: !!process.env['FIREBASE_PROJECT_ID'],
    env_client_email: !!process.env['FIREBASE_CLIENT_EMAIL'],
    env_private_key:  !!process.env['FIREBASE_PRIVATE_KEY'],
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
