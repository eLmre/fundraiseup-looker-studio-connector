/**
* Configuration for the script
* How to get Spreadsheet ID: https://stackoverflow.com/questions/36061433/how-do-i-locate-a-google-spreadsheet-id
* API Key optional, if key already saved in Google Script Properties, you can leave it empty.
*/

function runScheduledTask() {
  Logger.log('Run runScheduledTask()');

  var config = [
      {
          "spreadsheet_id": "",
          "sheet_name": "Fundraise Up",
          "api_key": ""
      },
  ];

  config.forEach(function(item) {
      updateSpreadsheet(item);
  });
}

function updateSpreadsheet(config) {
  Logger.log('Run updateSpreadsheet()');

  var spreadsheetId = config?.spreadsheet_id;
  var ss = SpreadsheetApp.openById(spreadsheetId);
  var api_key = config?.api_key || '';

  // select the sheet we want to update, or create it if it doesn't exist yet
  var sheet;
  var sheetName = config?.sheet_name || 'Fundraise Up';

  if ( ss.getSheetByName(sheetName) == null ) {
      sheet = ss.insertSheet(sheetName);
  } else {
      sheet = ss.getSheetByName(sheetName);
  }

  Logger.log('updateSpreadsheet() - Sheet name "'+ sheetName + '"');

  //define field schema, which will be added to the row headers
  var headers = [];

  var request = {};

  // In case we need to use another API key
  if (api_key) {
      request['api_key'] = api_key;
  }

  var schema = getSchema( request );
  request['fields'] = [];

  // add every field label to the headers array
  for ( var i = 0; i < schema.schema.length; i++ ) {
      headers.push(schema.schema[i].label);

      // request required data for buildRow() function
      request['fields'].push({
          'col_number': i + 1,
          'name': schema.schema[i].name,
          'letter': columnToLetter(i + 1),
          'spreadsheet_formula': schema.schema[i].spreadsheet_formula || '',
      });
  }

  // to be safe - remove duplicates from headers array
  headers = headers.filter(function(item, pos){
      return headers.indexOf(item) == pos;
  });

  if ( !headers.length ) {
      console.log('updateSpreadsheet() - Error: headers row not defined');
      return;
  }

  // add header row into spreadsheet
  var headerRow = sheet.getRange(1,1,1,headers.length);
  headerRow.setValues([headers]).setFontWeight("bold").setWrap(true);
  sheet.setFrozenRows(1);

  // Find latest value from column with header name "Donation ID". If value not found, return false.
  var donationIdIndex = headers.indexOf("Donation ID") + 1;

  if (donationIdIndex === 0) {
      console.log('updateSpreadsheet() - Error: "Donation ID" header not found');
      return false;
  }

  var last_row = sheet.getLastRow();

  if (last_row < 2) {
      console.log('updateSpreadsheet() - Warning: No data found in the "Donation ID" column');
  }

  request.last_row = last_row;

  if (last_row > 2) {
      var donationIdColumn = sheet.getRange(2, donationIdIndex, last_row - 1, 1).getValues();
      var latestValue = null;
      for (var i = donationIdColumn.length - 1; i >= 0; i--) {
          if (donationIdColumn[i][0]) {
              latestValue = donationIdColumn[i][0];
              break;
          }
      }

      if (latestValue === null) {
          console.log('updateSpreadsheet() - Error: No value found in the "Donation ID" column');
          return false;
      }

      console.log('updateSpreadsheet() - Latest Donation ID From Spreadsheet:', latestValue);

      request['ending_before'] = latestValue;
  }

  var data = fetchDataApi(request, true, false);

  if ( data.data === undefined || data.data.length === 0 ) {
    console.log( 'updateSpreadsheet() - Aborting - data not received' );
    return false;
  }

  // Let's reverse this to keep the date order, latest donations are added to the end of the sheet.
  // Make sure reverse before buildRows() so that the variables in formulas have the correct row number.
  data['data'] = data.data.map(function(r){return r;}).reverse();

  var rows = buildRows(request, data, 'spreadsheet');

  if( rows.length ) {
      sheet.getRange(last_row + 1, 1, rows.length, rows[0].length).setValues(rows);
  }
}

function columnToLetter(column) {
    var temp, letter = '';
    while (column > 0) {
        temp = (column - 1) % 26;
        letter = String.fromCharCode(temp + 65) + letter;
        column = (column - temp - 1) / 26;
    }
    return letter;
}

function letterToColumn(letter) {
    var column = 0, length = letter.length;
    for (var i = 0; i < length; i++) {
        column += (letter.charCodeAt(i) - 64) * Math.pow(26, length - i - 1);
    }
    return column;
}
