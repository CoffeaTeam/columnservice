// mongo localhost:27017/admin initialize.js
db.createUser(
  {
    user: "admin",
    pwd: "password",
    roles: [ { role: "userAdminAnyDatabase", db: "admin" }, "readWriteAnyDatabase" ]
  }
)
db.createUser(
  { 
    user: "coffea",
    pwd: "password",
    roles: [ { db: "coffeadb", role: "readWrite" } ]
  }
)
