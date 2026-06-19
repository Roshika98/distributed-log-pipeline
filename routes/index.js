const express = require("express");
const controller = require("../controllers");
const router = express.Router();

router.get("/logs", controller.getLogs);

module.exports = router;
