/**
 *@NApiVersion 2.1
 *@NScriptType MapReduceScript
 */
define(['N/file', 'N/record', 'N/search', 'N/format', 'N/log'],
function (file, record, search, format, log) {

    var PENDING_FOLDER_ID = 14390;
    var ERROR_FOLDER_ID = 14391;
    var PROCESSED_FOLDER_ID = 14392;

    var DEFAULT_TRANID_START_FROM = 70905;

    var IGNORE_DESCRIPTION_LIST = [
        'ULINE  *SHIP SUPPLIES',
        'WORLDWIDE EXPRESS',
        'Payment Thank You - Web'
    ];

    var CARD_ACCOUNT_MAP = {
        '2939': 244,
        '7794': 244,
        '8787': 244,
        '8998': 244,
        '3434': 244,
        '9848': 244,
        '2359': 244
    };

    function getInputData() {

        var pendingFiles = getPendingFiles();
        var allRows = [];
        var nextTranId = getNextTranIdNumber();

        log.audit('GET INPUT STARTED', {
            fileCount: pendingFiles.length,
            startingTranId: nextTranId
        });

        for (var i = 0; i < pendingFiles.length; i++) {

            try {

                var inputFile = file.load({
                    id: pendingFiles[i].id
                });

                var rows = parseTransactionFile(
                    inputFile.getContents(),
                    pendingFiles[i]
                );

                for (var r = 0; r < rows.length; r++) {

                    if (isIgnoredDescription(rows[r].Description)) {
                        continue;
                    }

                    rows[r].TranIdNumber = nextTranId;

                    nextTranId = incrementBigNumberString(nextTranId);

                    allRows.push(rows[r]);
                }

            } catch (e) {

                log.error('FILE FAILED', {
                    file: pendingFiles[i].name,
                    error: getErrorMessage(e)
                });
            }
        }

        return allRows;
    }

    function map(context) {

        var row = JSON.parse(context.value);

        try {

            var cardNo = cleanValue(row.Card);
            var description = cleanValue(row.Description);
            var category = cleanValue(row.Category);

            var rawAmount = parseSignedAmount(row.Amount);

            var amountPositive = Math.abs(rawAmount);

            var recordType = rawAmount < 0 ?
                'creditcardcharge' :
                'creditcardrefund';

            var creditCardAccountId = CARD_ACCOUNT_MAP[cardNo];

            if (!creditCardAccountId) {
                throw 'No Header Account Found For Card: ' + cardNo;
            }

            var entityInfo = getOtherNameEntityInfoFromDescription(description);

            if (!entityInfo || !entityInfo.id) {
                throw 'Vendor Not Found';
            }

            var expenseLine = getExpenseLineFromBackendMapping(cardNo, category);

            if (!expenseLine || !expenseLine.accountId) {
                throw 'Expense Account Mapping Not Found';
            }

            var classId = getHistoricalClass(entityInfo.id);

            var finalMemo = buildMainMemo(
                expenseLine.employeeName,
                description
            );

            var postDate = parseDate(row.PostDate);

            var postingPeriodId = getPostingPeriod(postDate);

            var ccRec = record.create({
                type: recordType,
                isDynamic: true
            });

            ccRec.setValue({
                fieldId: 'tranid',
                value: String(row.TranIdNumber)
            });

            ccRec.setValue({
                fieldId: 'entity',
                value: entityInfo.id
            });

            ccRec.setValue({
                fieldId: 'account',
                value: creditCardAccountId
            });

            ccRec.setValue({
                fieldId: 'usertotal',
                value: amountPositive
            });

            ccRec.setValue({
                fieldId: 'trandate',
                value: postDate
            });

            ccRec.setValue({
                fieldId: 'postingperiod',
                value: postingPeriodId
            });

            ccRec.setValue({
                fieldId: 'memo',
                value: finalMemo
            });

            if (classId) {

                ccRec.setValue({
                    fieldId: 'class',
                    value: classId
                });
            }

            ccRec.selectNewLine({
                sublistId: 'expense'
            });

            ccRec.setCurrentSublistValue({
                sublistId: 'expense',
                fieldId: 'account',
                value: expenseLine.accountId
            });

            ccRec.setCurrentSublistValue({
                sublistId: 'expense',
                fieldId: 'amount',
                value: amountPositive
            });

            ccRec.setCurrentSublistValue({
                sublistId: 'expense',
                fieldId: 'memo',
                value: finalMemo
            });

            if (classId) {

                ccRec.setCurrentSublistValue({
                    sublistId: 'expense',
                    fieldId: 'class',
                    value: classId
                });
            }

            ccRec.commitLine({
                sublistId: 'expense'
            });

            var recId = ccRec.save({
                enableSourcing: true,
                ignoreMandatoryFields: false
            });

            log.audit('TRANSACTION CREATED', {
                tranid: row.TranIdNumber,
                recordId: recId
            });

        } catch (e) {

            log.error('ROW FAILED', {
                line: row.LineNo,
                error: getErrorMessage(e)
            });
        }
    }

    function summarize(summary) {

        log.audit('SUMMARY COMPLETED', 'DONE');
    }

    function getExpenseLineFromBackendMapping(cardNo, category) {

        var mappingSearch = search.load({
            id: 'customsearch1779483018273'
        });

        mappingSearch.filters.push(search.createFilter({
            name: 'formulatext',
            formula: '{custrecord_card_number}',
            operator: search.Operator.IS,
            values: cardNo
        }));

        mappingSearch.filters.push(search.createFilter({
            name: 'formulatext',
            formula: '{custrecord_category}',
            operator: search.Operator.IS,
            values: category
        }));

        var results = mappingSearch.run().getRange({
            start: 0,
            end: 1
        });

        if (!results || results.length === 0) {
            return null;
        }

        return {
            accountId: results[0].getValue({
                name: 'custrecord_account_number'
            }),

            employeeName: results[0].getText({
                name: 'custrecord_employee_name'
            }) || ''
        };
    }

    function getHistoricalClass(entityId) {

        var ccSearch = search.load({
            id: 'customsearch1779482814225'
        });

        ccSearch.filters.push(search.createFilter({
            name: 'name',
            operator: search.Operator.ANYOF,
            values: entityId
        }));

        var results = ccSearch.run().getRange({
            start: 0,
            end: 1
        });

        if (!results || results.length === 0) {
            return '';
        }

        return results[0].getValue({
            name: 'class',
            summary: 'GROUP'
        }) || '';
    }

    function getOtherNameEntityInfoFromDescription(description) {

        var cleaned = cleanMerchantName(description);

        var entitySearch = search.create({
            type: 'othername',
            filters: [
                ['isinactive', 'is', 'F'],
                'AND',
                ['entityid', 'contains', cleaned]
            ],
            columns: [
                'internalid',
                'entityid'
            ]
        });

        var results = entitySearch.run().getRange({
            start: 0,
            end: 1
        });

        if (!results || results.length === 0) {
            return null;
        }

        return {
            id: results[0].getValue('internalid'),
            name: results[0].getValue('entityid')
        };
    }

    function getPendingFiles() {

        var files = [];

        var fileSearch = search.create({
            type: 'file',
            filters: [
                ['folder', 'anyof', PENDING_FOLDER_ID]
            ],
            columns: [
                search.createColumn({
                    name: 'internalid',
                    sort: search.Sort.ASC
                }),
                'name'
            ]
        });

        fileSearch.run().each(function(result){

            files.push({
                id: result.getValue('internalid'),
                name: result.getValue('name')
            });

            return true;
        });

        return files;
    }

    function parseTransactionFile(contents, sourceFile) {

        var lines = contents.split(/\r\n|\n|\r/);

        var data = [];

        var headerLine = '';
        var headerIndex = -1;

        for (var i = 0; i < lines.length; i++) {

            if (
                lines[i] &&
                lines[i].indexOf('Card') !== -1 &&
                lines[i].indexOf('Amount') !== -1
            ) {
                headerLine = lines[i];
                headerIndex = i;
                break;
            }
        }

        if (headerIndex === -1) {
            throw 'Header line not found';
        }

        var delimiter = headerLine.indexOf('\t') !== -1 ? '\t' : ',';

        var headers = parseDelimitedLine(headerLine, delimiter);

        for (var r = headerIndex + 1; r < lines.length; r++) {

            if (!lines[r] || !cleanValue(lines[r])) {
                continue;
            }

            var cols = parseDelimitedLine(lines[r], delimiter);

            var rawObj = {};

            for (var c = 0; c < headers.length; c++) {

                rawObj[
                    normalizeHeader(headers[c])
                ] = cols[c] || '';
            }

            data.push({
                SourceFileId: sourceFile.id,
                SourceFileName: sourceFile.name,
                LineNo: r + 1,
                Card: getColumn(rawObj, ['card']),
                TransactionDate: getColumn(rawObj, ['transactiondate']),
                PostDate: getColumn(rawObj, ['postdate']),
                Description: getColumn(rawObj, ['description']),
                Category: getColumn(rawObj, ['category']),
                Amount: getColumn(rawObj, ['amount'])
            });
        }

        return data;
    }

    function getNextTranIdNumber() {

        var tranSearch = search.create({
            type: search.Type.TRANSACTION,
            filters: [
                ['type', 'anyof', 'CardChrg', 'CardRfnd'],
                'AND',
                ['mainline', 'is', 'T']
            ],
            columns: [
                search.createColumn({
                    name: 'tranid',
                    sort: search.Sort.DESC
                })
            ]
        });

        var results = tranSearch.run().getRange({
            start: 0,
            end: 1
        });

        if (results && results.length > 0) {

            var tranIdText = cleanValue(
                results[0].getValue('tranid')
            );

            if (/^[0-9]+$/.test(tranIdText)) {

                return incrementBigNumberString(tranIdText);
            }
        }

        return String(DEFAULT_TRANID_START_FROM);
    }

    function getPostingPeriod(dateObj) {

        var dateText = format.format({
            value: dateObj,
            type: format.Type.DATE
        });

        var periodSearch = search.create({
            type: 'accountingperiod',
            filters: [
                ['startdate', 'onorbefore', dateText],
                'AND',
                ['enddate', 'onorafter', dateText],
                'AND',
                ['isyear', 'is', 'F'],
                'AND',
                ['isquarter', 'is', 'F']
            ],
            columns: ['internalid']
        });

        var result = periodSearch.run().getRange({
            start: 0,
            end: 1
        });

        if (result && result.length > 0) {

            return result[0].getValue('internalid');
        }

        throw 'Posting Period Not Found';
    }

    function buildMainMemo(employeeName, description) {

        employeeName = cleanValue(employeeName);

        description = cleanValue(description);

        if (employeeName && description) {

            return employeeName + ' - ' + description;
        }

        return description || '';
    }

    function isIgnoredDescription(description) {

        var desc = cleanValue(description).toLowerCase();

        for (var i = 0; i < IGNORE_DESCRIPTION_LIST.length; i++) {

            var ignoreText = cleanValue(
                IGNORE_DESCRIPTION_LIST[i]
            ).toLowerCase();

            if (
                desc === ignoreText ||
                desc.indexOf(ignoreText) !== -1
            ) {
                return true;
            }
        }

        return false;
    }

    function cleanMerchantName(value) {

        return cleanValue(value)
            .replace(/&amp;/g, '&')
            .replace(/\*/g, ' ')
            .replace(/\./g, ' ')
            .replace(/\s+/g, ' ');
    }

    function parseDelimitedLine(line, delimiter) {

        var result = [];
        var current = '';
        var inQuotes = false;

        for (var i = 0; i < line.length; i++) {

            var ch = line.charAt(i);

            if (ch === '"') {

                if (
                    inQuotes &&
                    line.charAt(i + 1) === '"'
                ) {
                    current += '"';
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }

            } else if (ch === delimiter && !inQuotes) {

                result.push(current);
                current = '';

            } else {

                current += ch;
            }
        }

        result.push(current);

        return result;
    }

    function getColumn(obj, names) {

        for (var i = 0; i < names.length; i++) {

            var key = normalizeHeader(names[i]);

            if (obj.hasOwnProperty(key)) {

                return obj[key];
            }
        }

        return '';
    }

    function normalizeHeader(value) {

        return cleanValue(value)
            .toLowerCase()
            .replace(/\s+/g, '');
    }

    function cleanValue(value) {

        if (value === null || value === undefined) {
            return '';
        }

        return String(value)
            .replace(/^\s+|\s+$/g, '')
            .replace(/\s+/g, ' ');
    }

    function parseSignedAmount(value) {

        var text = cleanValue(value)
            .replace(/\$/g, '')
            .replace(/,/g, '');

        if (
            text.charAt(0) === '(' &&
            text.charAt(text.length - 1) === ')'
        ) {
            text = '-' + text.substring(1, text.length - 1);
        }

        var amount = parseFloat(text);

        if (isNaN(amount)) {
            throw 'Invalid Amount';
        }

        return amount;
    }

    function parseDate(value) {

        var parts = cleanValue(value).split('/');

        return new Date(
            parseInt(parts[2], 10),
            parseInt(parts[0], 10) - 1,
            parseInt(parts[1], 10)
        );
    }

    function incrementBigNumberString(value) {

        var digits = cleanValue(value).split('');

        var carry = 1;

        for (var i = digits.length - 1; i >= 0; i--) {

            var num = parseInt(digits[i], 10) + carry;

            if (num === 10) {

                digits[i] = '0';

            } else {

                digits[i] = String(num);

                carry = 0;

                break;
            }
        }

        if (carry === 1) {
            digits.unshift('1');
        }

        return digits.join('');
    }

    function getErrorMessage(e) {

        if (!e) return '';

        if (typeof e === 'string') return e;

        if (e.message) return e.message;

        return JSON.stringify(e);
    }

    return {
        getInputData: getInputData,
        map: map,
        summarize: summarize
    };

});