var fs = require('fs');
var csv = require('fast-csv');

module.exports = (function() {
  var csvUtms = './csv/utms.csv',
      csvUsers = './csv/users_skus.csv',
      csvMerged = './dist/users_skus_utms.csv',
      arrUTMS = [],
      arrUsers = [],
      arrMerged = [],
      incrementDefault = 1680,
      countUTM = 0,
      limitUTM = 100,
      countUSERS = 0,
      limitUSERS = incrementDefault;

  function CSV() {}

  CSV.build = function() {
    read_utms();
  }

  // private method
  function read_utms() {
    csv
      .fromPath(csvUtms)
      .on('data', function(data){
        arrUTMS.push(data[0]);
      })
      .on('end', function() {
        read_users();
      });
  };

  // private method
  function read_users() {
    csv
      .fromPath(csvUsers)
      .on('data', function(data){
        arrUsers.push(data);
      })
      .on('end', function() {
        mergeData();
      });
  };

  // private method
  function mergeData(callback) {
    while(countUTM < limitUTM) {
      var utm = arrUTMS[countUTM];

      while(countUSERS < limitUSERS) {
        var user = arrUsers[countUSERS];
        user.push(utm);
        arrMerged.push(user);

        countUSERS++;
      }

      countUTM++;
      countUSERS = limitUSERS;
      limitUSERS += incrementDefault;
    }

    write(arrMerged);
  }

  // private method
  function write(arr) {
    var ws = fs.createWriteStream(csvMerged);

    csv
      .write(arr, {headers: true})
      .pipe(ws);
  }

  return CSV;
})();