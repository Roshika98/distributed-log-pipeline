const service = require("../services");

exports.getLogs = async (req, res) => {
  const {
    startTime,
    endTime,
    service: serviceName,
    level,
    limit = 100,
    offset = 0,
  } = req.query;

  if (!startTime || !endTime) {
    return res
      .status(400)
      .json({ error: "startTime and endTime are required" });
  }

  const params = {
    startTime,
    endTime,
    service: serviceName,
    level,
    limit: parseInt(limit),
    offset: parseInt(offset),
  };

  const results = await service.getLogs(params);
  res.json(results);
};
