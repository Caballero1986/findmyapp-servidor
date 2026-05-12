const express   = require('express');
const http      = require('http');
const { Server } = require('socket.io');
const cors      = require('cors');
const admin     = require('firebase-admin');

// Inicializar Firebase Admin para enviar notificaciones push
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const app      = express();
const servidor = http.createServer(app);
const io       = new Server(servidor, {
  cors: { origin: '*' },
  pingTimeout:  20000,
  pingInterval: 10000,
});

app.use(cors());
app.use(express.json());

const grupos      = {};
const nombreIndex = {};

// Tokens FCM de cada usuario — para enviar notificaciones push
// fcmTokens[grupo][nombre] = token
const fcmTokens = {};

function asegurarGrupo(grupo) {
  if (!grupos[grupo])      grupos[grupo]      = {};
  if (!nombreIndex[grupo]) nombreIndex[grupo] = {};
  if (!fcmTokens[grupo])   fcmTokens[grupo]   = {};
}

io.on('connection', socket => {
  let miGrupo  = null;
  let miNombre = null;

  socket.on('unirse_grupo', datos => {
    miGrupo  = datos.codigoGrupo;
    miNombre = datos.nombre || 'Usuario';

    socket.join(miGrupo);
    asegurarGrupo(miGrupo);

    // Guardar token FCM si viene en los datos
    if (datos.fcmToken) {
      fcmTokens[miGrupo][miNombre] = datos.fcmToken;
      console.log('Token FCM guardado para:', miNombre);
    }

    // Eliminar conexión duplicada
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

    asegurarGrupo(miGrupo);
    nombreIndex[miGrupo][miNombre] = socket.id;

    socket.emit('ubicaciones_iniciales',
      Object.values(grupos[miGrupo]));

    console.log(miNombre, 'se unio al grupo', miGrupo);
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
    socket.to(miGrupo).emit('ubicacion_actualizada',
      grupos[miGrupo][socket.id]);
  });

  // Botón 🔄 — pedir actualización a todos del grupo
  socket.on('pedir_actualizacion', async () => {
    if (!miGrupo) return;

    // 1. Avisar a los que están conectados via Socket.IO
    io.to(miGrupo).emit('forzar_actualizacion');

    // 2. Enviar notificación push a los que tienen token FCM
    const tokens = Object.values(fcmTokens[miGrupo] || {})
      .filter(t => t); // filtrar tokens vacíos

    if (tokens.length === 0) return;

    console.log('Enviando push a', tokens.length, 'usuarios');

    for (const token of tokens) {
      try {
        await admin.messaging().send({
          token: token,
          // Notificación silenciosa — solo activa el servicio
          // no muestra alerta al usuario
          data: {
            tipo:   'pedir_ubicacion',
            grupo:  miGrupo,
            origen: miNombre,
          },
          android: {
            priority: 'high',
            // Sin notificación visible — solo datos
          },
        });
        console.log('Push enviado OK');
      } catch (err) {
        console.log('Error enviando push:', err.message);
        // Si el token es inválido, eliminarlo
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

    if (grupos[miGrupo]) {
      delete grupos[miGrupo][socket.id];
    }

    if (miNombre && nombreIndex[miGrupo] &&
        nombreIndex[miGrupo][miNombre] === socket.id) {
      delete nombreIndex[miGrupo][miNombre];
    }

    if (grupos[miGrupo] &&
        Object.keys(grupos[miGrupo]).length === 0) {
      delete grupos[miGrupo];
      delete nombreIndex[miGrupo];
      delete fcmTokens[miGrupo];
    }

    io.to(miGrupo).emit('usuario_desconectado', socket.id);
  });
});

app.get('/', (req, res) =>
  res.json({
    estado: 'FindMyApp corriendo',
    grupos: Object.keys(grupos).length,
    hora:   new Date().toISOString(),
  })
);

const PORT = process.env.PORT || 3000;
servidor.listen(PORT, () =>
  console.log('Servidor en puerto', PORT)
);