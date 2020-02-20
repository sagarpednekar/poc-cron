const mongoose = require("mongoose");

module.exports = {
  connnect: () => {
    mongoose.connect("mongodb://localhost/test", {
      useNewUrlParser: true
    });
    const db = mongoose.connection;
    db.on("connected", () => {
      console.log("DB connnected ....");
    });
    db.on("error", () => {
      console.log("Error connecting database ....");
    });
  },
  Schema: mongoose.Schema,
  model: (modelName, schemaName) => {
    return mongoose.model(modelName, schemaName);
  }
};
