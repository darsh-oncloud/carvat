/**
 *@NApiVersion 2.1
 *@NScriptType MapReduceScript
 */
define(['N/file', 'N/record', 'N/search', 'N/format', 'N/log'],
function (file, record, search, format, log) {

    var PENDING_FOLDER_ID = 14390;
    var PROCESSED_FOLDER_ID = 14392;

    var CARD_ACCOUNT_MAP = {
        '2939': 244,
        '7794': 244,
        '8787': 244,
        '8998': 244,
        '3434': 244,
        '9848': 244
    };

    function getInputData() {

        var rows = [];

        var fileSearch = search.create({
            type: 'file',
            filters: [['folder', 'anyof', PENDING_FOLDER_ID]],
            columns: ['internalid', 'name']
        });

        fileSearch.run().each(function(result){

            var fileObj = file.load({
                id: result.getValue('internalid')
            });

            var contents = fileObj.getContents().split(/\r\n|\n/);

            var headers = contents[0].split(',');

            for (var i = 1; i < contents.length; i++) {

                if (!contents[i]) {
                    continue;
                }

                var cols = contents[i].split(',');

                rows.push({
                    fileId: result.getValue('internalid'),
                    fileName: result.getValue('name'),
                    Card: cols[0],
                    TransactionDate: cols[1],
                    PostDate: cols[2],
                    Description: cols[3],
                    Category: cols[4],
                    Amount: cols[5]
                });
            }

            return true;
        });

        return rows;
    }

    function map(context) {

        var row = JSON.parse(context.value);

        try {

            var cardNo = row.Card;
            var category = row.Category;
            var description = row.Description;

            var amount = parseFloat(row.Amount);

            var headerAccount = CARD_ACCOUNT_MAP[cardNo];

            if (!headerAccount) {
                throw 'No Header Account Mapping Found';
            }

            var mappingData = getExpenseAccount(cardNo, category);

            if (!mappingData.accountId) {
                throw 'Expense Account Mapping Not Found';
            }

            var vendorData = getVendor(description);

            if (!vendorData.vendorId) {
                throw 'Vendor Not Found';
            }

            var classId = getHistoricalClass(vendorData.vendorId);

            var memoText = mappingData.employeeName ?
                mappingData.employeeName + ' - ' + description :
                description;

            var recType = amount < 0 ?
                'creditcardcharge' :
                'creditcardrefund';

            var rec = record.create({
                type: recType,
                isDynamic: true
            });

            rec.setValue({
                fieldId: 'account',
                value: headerAccount
            });

            rec.setValue({
                fieldId: 'entity',
                value: vendorData.vendorId
            });

            rec.setValue({
                fieldId: 'trandate',
                value: parseDate(row.PostDate)
            });

            rec.setValue({
                fieldId: 'memo',
                value: memoText
            });

            rec.setValue({
                fieldId: 'usertotal',
                value: Math.abs(amount)
            });

            if (classId) {
                rec.setValue({
                    fieldId: 'class',
                    value: classId
                });
            }

            rec.selectNewLine({
                sublistId: 'expense'
            });

            rec.setCurrentSublistValue({
                sublistId: 'expense',
                fieldId: 'account',
                value: mappingData.accountId
            });

            rec.setCurrentSublistValue({
                sublistId: 'expense',
                fieldId: 'amount',
                value: Math.abs(amount)
            });

            rec.setCurrentSublistValue({
                sublistId: 'expense',
                fieldId: 'memo',
                value: memoText
            });

            if (classId) {
                rec.setCurrentSublistValue({
                    sublistId: 'expense',
                    fieldId: 'class',
                    value: classId
                });
            }

            rec.commitLine({
                sublistId: 'expense'
            });

            var recId = rec.save();

            log.audit('Transaction Created', recId);

        } catch (e) {

            log.error('Row Failed', {
                row: row,
                error: e
            });
        }
    }

    function getExpenseAccount(cardNo, category) {

        var obj = {};

        var searchObj = search.create({
            type: 'customrecord_credit_card_backend_mapping',
            filters: [
        ["formulatext: {custrecord_card_number}", "is", cardNo],
        "AND",
        ["formulatext: {custrecord_category}", "is", category]
    ],
            columns: [
                'custrecord_account_number',
                'custrecord_employee_name'
            ]
        });

        searchObj.run().each(function(result){

            obj.accountId = result.getValue('custrecord_account_number');

            obj.employeeName = result.getText('custrecord_employee_name');

            return false;
        });

        return obj;
    }

    function getVendor(description) {

        var obj = {};

        var vendorSearch = search.create({
            type: 'vendor',
            filters: [
                ['entityid', 'contains', description]
            ],
            columns: ['internalid']
        });

        vendorSearch.run().each(function(result){

            obj.vendorId = result.getValue('internalid');

            return false;
        });

        return obj;
    }

    function getHistoricalClass(vendorId) {

        var classId = '';

        var ccSearch = search.create({
            type: 'creditcardcharge',
            filters: [
                ['type','anyof','CardChrg'],
                'AND',
                ['mainline','is','T'],
                'AND',
                ['name','anyof', vendorId],
                'AND',
                ['class','noneof','@NONE@']
            ],
            columns: [
                search.createColumn({
                    name: 'class',
                    summary: 'GROUP'
                })
            ]
        });

        ccSearch.run().each(function(result){

            classId = result.getValue({
                name: 'class',
                summary: 'GROUP'
            });

            return false;
        });

        return classId;
    }

    function summarize(summary) {

        log.audit('Summary Completed', 'Done');

    }

    function parseDate(value) {

        var parts = value.split('/');

        return new Date(
            parseInt(parts[2], 10),
            parseInt(parts[0], 10) - 1,
            parseInt(parts[1], 10)
        );
    }

    return {
        getInputData: getInputData,
        map: map,
        summarize: summarize
    };

});