const repository = require("../repositories");

const getLogs = async (params) => {
  return await repository.getLogs(params);
};

module.exports = {
  getLogs,
};
