const admin = require('firebase-admin');

if (!admin.apps.length) {
  var serviceAccount = require("../key.json");

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: "https://portafolio-inversiones-default-rtdb.firebaseio.com"
  });
}

module.exports = admin;