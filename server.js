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
    const admin = require('firebase-admin');
    admin.initializeApp({
      credential: admin.credential.cert({
        projectId:   'findmyapp-5dd91',
        clientEmail: 'firebase-adminsdk-fbsvc@findmyapp-5dd91.iam.gserviceaccount.com',
        privateKey:  '-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC9xNezFPr0Mdo6\nwJPGoCr98ktLTwtQqROUqik2Qn+cBV8M087qtAQzMFCNU0B1FlYSjnbKdR4BK6nU\nGphLq+cMbbnesKanCIupgz3h6TMAJkUQeS743WEFN1B0hMZ+fNMcMEjJs/3YnKud\nIpwBZrL7vv9s4yKBs4n2vBVpzoSA2iDfU0g+ZRK6e91Q/EDHsTgbOYTudwiVufxx\nAESNXqYo2i7X6CrAKK5Egy9Sh39O6uuLi05Q+lPhJkrEvuxgF/6tiDhW0EYXO1XK\neQ0UomLGSps5x8HzU78IxxUXS7devz6y9rHtOlkSmGfioFabpRjmnlVtuR3ymuLK\nZS4eyyaLAgMBAAECggEABl6N/X/DGzZ1JFWvCr1OKW8xdxn0akT9LZc3LOVh0m3H\nuUQpWhf9WsPQtahJWI9jB4UVQvRYCSKwX43I9iPolwfec4qq/UjnJHhi4lXpFU8z\nhZBbsun98KaLom2LxVZmi41UqW3LeQgWe9QGjRwnqRV0Y0Y4XpwVEbKnEVcSJspH\nSCk25I/lDvt8PTcXi4eGqQUBfoRUvo5mjQflaatW+lXcKP/pe7tL5XMnx9CwP6T+\nig5F3ZbokLtU917CFbaMJK7CLMRpkBzp02/bWYF0aiNIt5wNOoI6v7xmo7ubA2Du\nDUWsRj5GvG8OEVPk6haK1xLDcIhchk76d+0yqIdnoQKBgQDwZkWkYVYloGZqLCOv\ns15hSQWQwg8hszUe3Fn5XwuyYTOUQrRG3hZXDBwwKsdMaR4nOKhd8rMdjsMQ5QiF\ndVniD38Qua7IjU1wX7SGB36rawVl6I0SPbMdRF2B8EHv9Q1t7lQkugztCK6yKcfT\nDg44BCMFghU+5d2XdmEk80qTGwKBgQDKFXNjzV8eRJPGeA8PoUDHKZr4EdBN5Te7\nKMA73oPpiY+UjB3WgH3/dabTDNnVhWhswqxTlHXm4BMJJRGraSL+BWTNW2xmyIaY\ncsHgKD2F+w7gBa+XPjStfKpREjdaNdV+lFR5RQk619rBMwMA78MSqbvYF0R4hHnb\nnPyiv1aBUQKBgQDRehzHPzYE9X8olyvFh4P/O9UIrcDnubPZH2Obg7G6jy6Vy4Yx\nloZa9ad+ZVhjAdoPjTRRIRHo+KSLjTfeq0JWVPCBD3v5L//BovO4MsGy9z+t7HU4\n5uCz6QWeG1ApmxxHpXxWwHRQ+9bWUsfX5hCHyTsHKrH+q+hqefHuc72Q7wKBgQCd\ncNbkk5WerEkBYHpDcBtaz3RX/vDuLz4bR6V1P2hAY5cEYaHBg3wmsg/V7/Yq33Q6\n//RobYrp4/uQrVu10wSbKbKeN7Md5O7QCOA5dsBwCOhO6r8hsyoxW6YJ4YpUcwqx\nQnkJvhpxyMg6qzaU3Td669hYg9ApoPSbgBRn63BG8QKBgQDoEw17eoIz0FG6YQte\nz2sEUGy/tfGVM0gnGvcAJBfiYyuyRL3Nv6SGFpO8DjnQCYcRiJjeLgx07hVo0XGx\nurDSJcJ+mXUPWOXyNQgxsdIAoW6PPAKe/KoObP1msEhyJ76bSPANpfvx82pH3cbE\nscXy0rbb7CWADKMQz9U2/Uan7g==\n-----END PRIVATE KEY-----\n'.replace(/\\n/g, '\n'),
      }),
    });
    adminApp = admin;
    console.log('[FCM] Firebase Admin inicializado OK');
  } catch (e) {
    console.log('[FCM] Error:', e.message);
    adminApp = null;
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
