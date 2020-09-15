// Mocha does not support multiple reporters running at once. So we use this
// simple wrapper as suggested in:
// https://github.com/mochajs/mocha/pull/1360#issuecomment-407404831

const mocha = require('mocha');

function MultiReporter (runner, options) {
  this.reports = [];
  if (!options.reporterOptions.reporters) {
    console.log('\nneeds --reporter-options reporters="SPACE_SEPARATED_MOCHA_REPORTS"');
    return;
  }
  const self = this;
  options.reporterOptions.reporters.split(' ').forEach(function (report) {
    const ReportClass = mocha.reporters[report];
    if (!ReportClass) {
      console.log('\ninvalid report class available: ' + Object.keys(mocha.reporters).join(','));
      return;
    }
    const reportInstance = new ReportClass(runner, options);
    self.reports.push(reportInstance);
  });
}

MultiReporter.prototype.epilogue = function () {
  this.reports.forEach(function (reportInstance) {
    reportInstance.epilogue();
  });
};

exports = module.exports = MultiReporter;
