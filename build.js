(function (){
	const fs = require('fs');
	// const jsonData = require('../erddaps.json');
	const jsonData = require('./noaa_wide_full.json');
	const outData = 'var awesomeErddaps = ' + JSON.stringify(jsonData) + ';';
	const jsonpData = 'awesomeErddapsCb(' + JSON.stringify(jsonData) + ');';
	const writeFile = (filename,data) => {
          fs.writeFile(filename, data, function(err) {
		if(err) {
			return console.log(err);
		}
		else {
			console.log(`[awesome-erddap] ${filename} re-written...`);
		}
	  }); 
       };
       writeFile("noaa_wide_full.js",outData);
       writeFile("noaa_wide_full.jsonp",jsonpData);
})();
