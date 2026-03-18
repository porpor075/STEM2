/**
 * STEM++ Google Apps Script for Calling List Integration (v5 - with Progress Tier)
 */

function doPost(e) {
  var data;
  try {
    data = JSON.parse(e.postData.contents);
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({"status": "error", "message": "Invalid JSON"}))
      .setMimeType(ContentService.MimeType.JSON);
  }

  var action = data.action;
  var sheetName = "Calling List"; 
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(sheetName);
  
  // Headers: Email(1), First Name(2), Last Name(3), Phone(4), Status(5), Timestamp(6), By User(7), Learning Status(8), Density(9), Customer Type(10), Note(11), Progress(12)
  var headers = ["Email", "First Name", "Last Name", "Phone", "Status", "Timestamp", "By User", "Learning Status", "Density", "Customer Type", "Note", "Progress"];

  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
    sheet.appendRow(headers);
    sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold").setBackground("#f3f3f3");
    sheet.setFrozenRows(1);
  }

  if (action === "batchAddUsers") {
    var users = data.users;
    var lastRow = sheet.getLastRow();
    var emailToRow = {};
    
    if (lastRow > 1) {
      var dataRange = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
      for (var i = 0; i < dataRange.length; i++) {
        emailToRow[String(dataRange[i][0]).toLowerCase().trim()] = i + 2;
      }
    }
    
    var addedCount = 0;
    var updatedCount = 0;
    var rowsToAppend = [];
    
    users.forEach(function(user) {
      var email = String(user.email).toLowerCase().trim();
      if (!email) return;
      
      if (emailToRow[email]) {
        var rowNum = emailToRow[email];
        // Update Metadata: Learning Status(8), Density(9), Progress(12)
        sheet.getRange(rowNum, 8).setValue(user.learningStatus || "-");
        sheet.getRange(rowNum, 9).setValue(user.density || "1");
        sheet.getRange(rowNum, 12).setValue(user.progressTier || "0%");
        updatedCount++;
      } else {
        rowsToAppend.push([
          user.email, user.firstName, user.lastName, user.phone || "-", 
          "Pending", new Date(), data.username || "System",
          user.learningStatus || "-", user.density || "1", "B2C", "", user.progressTier || "0%"
        ]);
        addedCount++;
      }
    });
    
    if (rowsToAppend.length > 0) {
      sheet.getRange(sheet.getLastRow() + 1, 1, rowsToAppend.length, headers.length).setValues(rowsToAppend);
    }
    
    return jsonResponse({ "status": "success", "message": "เพิ่ม " + addedCount + " คน, อัปเดตข้อมูล " + updatedCount + " คน" });
  } 
  
  else if (action === "logCall") {
    var email = String(data.email).toLowerCase().trim();
    var lastRow = sheet.getLastRow();
    var found = false;
    
    if (lastRow > 1) {
      var emails = sheet.getRange(2, 1, lastRow - 1, 1).getValues().flat();
      for (var i = 0; i < emails.length; i++) {
        if (String(emails[i]).toLowerCase().trim() === email) {
          var rowNum = i + 2;
          // Update core call data
          sheet.getRange(rowNum, 4, 1, 4).setValues([[ data.phone, data.status, data.timestamp, data.username ]]);
          if (data.customerType) sheet.getRange(rowNum, 10).setValue(data.customerType);
          if (data.note !== undefined) sheet.getRange(rowNum, 11).setValue(data.note);
          if (data.progressTier) sheet.getRange(rowNum, 12).setValue(data.progressTier);
          found = true;
          break;
        }
      }
    }
    
    if (!found) {
      sheet.appendRow([
        data.email, data.firstName, data.lastName, data.phone, 
        data.status, data.timestamp, data.username, "-", "1", 
        data.customerType || "B2C", data.note || "", data.progressTier || "0%"
      ]);
    }
    return jsonResponse({ "status": "success" });
  }

  return jsonResponse({ "status": "error", "message": "Unknown Action" });
}

function jsonResponse(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}

function doGet() { return ContentService.createTextOutput("STEM++ API v5 Active!"); }
