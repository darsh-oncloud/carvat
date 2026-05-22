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

    var MAPPING_RECORD_TYPE = 'customrecord_credit_card_backend_mapping';

    var MAP_EMPLOYEE_NAME_FIELD = 'custrecord_employee_name';
    var MAP_CARD_NUMBER_FIELD = 'custrecord_card_number';
    var MAP_CATEGORY_FIELD = 'custrecord_category';
    var MAP_ACCOUNT_NUMBER_FIELD = 'custrecord_account_number';

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
        '9848': 244
    };

    function getInputData() {
        var pendingFiles = getPendingFiles();
        var allRows = [];
        var nextTranId = getNextTranIdNumber();

        if (!pendingFiles || pendingFiles.length === 0) {
            log.audit('No Pending Files Found', PENDING_FOLDER_ID);
            return [];
        }

        log.audit('Starting Tran ID Sequence', nextTranId);

        for (var i = 0; i < pendingFiles.length; i++) {
            try {
                var inputFile = file.load({ id: pendingFiles[i].id });

                log.audit('Processing Pending File', {
                    fileId: pendingFiles[i].id,
                    fileName: pendingFiles[i].name
                });

                var rows = parseTransactionFile(inputFile.getContents(), pendingFiles[i]);

                if (!rows || rows.length === 0) {
                    allRows.push({
                        SourceFileId: pendingFiles[i].id,
                        SourceFileName: pendingFiles[i].name,
                        LineNo: '',
                        Card: '',
                        TransactionDate: '',
                        PostDate: '',
                        Description: '',
                        Amount: '',
                        TranIdNumber: '',
                        InputError: 'No data rows found in file.'
                    });
                    continue;
                }

                for (var r = 0; r < rows.length; r++) {
                    var row = rows[r];

                    if (isIgnoredDescription(row.Description)) {
                        row.IsSkippedLine = true;
                        row.TranIdNumber = '';
                        row.InputError = 'Skipped description, transaction not created: ' + row.Description;
                        allRows.push(row);
                        continue;
                    }

                    var validationError = validateRow(row);

                    if (validationError) {
                        row.TranIdNumber = '';
                        row.InputError = validationError;
                        allRows.push(row);
                        continue;
                    }

                    row.TranIdNumber = nextTranId;
                    nextTranId++;

                    allRows.push(row);
                }

            } catch (e) {
                log.error('Input File Failed', {
                    file: pendingFiles[i],
                    error: getErrorMessage(e)
                });

                allRows.push({
                    SourceFileId: pendingFiles[i].id,
                    SourceFileName: pendingFiles[i].name,
                    LineNo: '',
                    Card: '',
                    TransactionDate: '',
                    PostDate: '',
                    Description: '',
                    Amount: '',
                    TranIdNumber: '',
                    InputError: getErrorMessage(e)
                });
            }
        }

        return allRows;
    }

    function map(context) {
        var row = JSON.parse(context.value);

        try {
            if (row.InputError) {
                throw row.InputError;
            }

            var cardNo = cleanValue(row.Card);
            var description = cleanValue(row.Description);

            var creditCardAccountId = CARD_ACCOUNT_MAP[cardNo];

            if (!creditCardAccountId) {
                throw 'No header credit card account mapping found for card number: ' + cardNo;
            }

            var rawAmount = parseSignedAmount(row.Amount);
            var amountPositive = Math.abs(rawAmount);
            var recordType = rawAmount < 0 ? 'creditcardcharge' : 'creditcardrefund';

            var postDate = parseDate(row.PostDate);
            var postingPeriodId = getPostingPeriod(postDate);

            var mapping = getCardMapping(cardNo);

            var expenseAccountId = '';
            var classId = '';
            var employeeName = '';

            if (mapping && mapping.expenseAccountId) {
                expenseAccountId = mapping.expenseAccountId;
                employeeName = mapping.employeeName || '';
            } else {
                var historicalLine = getHistoricalExpenseLine(description);

                if (!historicalLine || !historicalLine.accountId) {
                    throw 'No custom mapping or historical expense account found for card/description: ' + cardNo + ' / ' + description;
                }

                expenseAccountId = historicalLine.accountId;
                classId = historicalLine.classId || '';
            }

            var memoText = employeeName ? employeeName + ' - ' + description : description;

            var ccRec = record.create({
                type: recordType,
                isDynamic: true
            });

            ccRec.setValue({
                fieldId: 'tranid',
                value: String(row.TranIdNumber)
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
                value: memoText
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
                value: expenseAccountId
            });

            ccRec.setCurrentSublistValue({
                sublistId: 'expense',
                fieldId: 'amount',
                value: amountPositive
            });

            ccRec.setCurrentSublistValue({
                sublistId: 'expense',
                fieldId: 'memo',
                value: memoText
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

            log.audit('Credit Card Transaction Created', {
                line: row.LineNo,
                tranid: row.TranIdNumber,
                recordType: recordType,
                recordId: recId
            });

            context.write({
                key: 'SUCCESS',
                value: JSON.stringify({
                    SourceFileId: row.SourceFileId,
                    SourceFileName: row.SourceFileName,
                    LineNo: row.LineNo,
                    TranIdNumber: row.TranIdNumber,
                    RecordId: recId
                })
            });

        } catch (e) {
            row.ErrorMessage = getErrorMessage(e);

            log.error('Credit Card Row Failed', {
                line: row.LineNo,
                tranid: row.TranIdNumber,
                error: row.ErrorMessage
            });

            context.write({
                key: 'ERROR',
                value: JSON.stringify(row)
            });
        }
    }

    function summarize(summary) {
        var fileStatus = {};

        summary.output.iterator().each(function (key, value) {
            var obj = JSON.parse(value);
            var fileId = obj.SourceFileId;

            if (!fileId) {
                return true;
            }

            if (!fileStatus[fileId]) {
                fileStatus[fileId] = {
                    fileId: fileId,
                    fileName: obj.SourceFileName || '',
                    successCount: 0,
                    errorRows: []
                };
            }

            if (key === 'SUCCESS') {
                fileStatus[fileId].successCount++;
            }

            if (key === 'ERROR') {
                fileStatus[fileId].errorRows.push(obj);
            }

            return true;
        });

        summary.mapSummary.errors.iterator().each(function (key, error) {
            log.error('Map Summary Error', {
                key: key,
                error: error
            });
            return true;
        });

        for (var fileId in fileStatus) {
            if (fileStatus.hasOwnProperty(fileId)) {

                if (fileStatus[fileId].errorRows.length > 0) {
                    createErrorFile(fileStatus[fileId]);
                }

                moveFileToFolder(fileId, PROCESSED_FOLDER_ID);

                log.audit('File Processing Completed', {
                    fileId: fileId,
                    fileName: fileStatus[fileId].fileName,
                    successCount: fileStatus[fileId].successCount,
                    errorCount: fileStatus[fileId].errorRows.length
                });
            }
        }
    }

    function getCardMapping(cardNo) {
        cardNo = cleanValue(cardNo);

        var mappingSearch = search.create({
            type: MAPPING_RECORD_TYPE,
            filters: [
                [MAP_CARD_NUMBER_FIELD, 'is', cardNo],
                'AND',
                ['isinactive', 'is', 'F']
            ],
            columns: [
                search.createColumn({ name: MAP_EMPLOYEE_NAME_FIELD }),
                search.createColumn({ name: MAP_CARD_NUMBER_FIELD }),
                search.createColumn({ name: MAP_CATEGORY_FIELD }),
                search.createColumn({ name: MAP_ACCOUNT_NUMBER_FIELD })
            ]
        });

        var results = mappingSearch.run().getRange({
            start: 0,
            end: 1
        });

        if (!results || results.length === 0) {
            return null;
        }

        return {
            employeeName: results[0].getValue({ name: MAP_EMPLOYEE_NAME_FIELD }),
            cardNumber: results[0].getValue({ name: MAP_CARD_NUMBER_FIELD }),
            category: results[0].getValue({ name: MAP_CATEGORY_FIELD }),
            expenseAccountId: results[0].getValue({ name: MAP_ACCOUNT_NUMBER_FIELD })
        };
    }

    function getNextTranIdNumber() {
        var maxTranId = 0;

        var tranSearch = search.create({
            type: search.Type.TRANSACTION,
            filters: [
                ['type', 'anyof', 'CardChrg', 'CardRfnd'],
                'AND',
                ['mainline', 'is', 'T'],
                'AND',
                ['tranid', 'isnotempty', '']
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
            var tranIdText = cleanValue(results[0].getValue({ name: 'tranid' }));
            var tranIdNumber = parseInt(tranIdText, 10);

            if (!isNaN(tranIdNumber)) {
                maxTranId = tranIdNumber;
            }
        }

        if (maxTranId <= 0) {
            maxTranId = DEFAULT_TRANID_START_FROM - 1;
        }

        return maxTranId + 1;
    }

    function validateRow(row) {
        if (!cleanValue(row.Card)) return 'Missing Card Number';
        if (!cleanValue(row.Description)) return 'Missing Description';
        if (!cleanValue(row.PostDate)) return 'Missing Post Date';
        if (!cleanValue(row.Amount)) return 'Missing Amount';

        try {
            parseSignedAmount(row.Amount);
        } catch (e) {
            return getErrorMessage(e);
        }

        try {
            parseDate(row.PostDate);
        } catch (e2) {
            return getErrorMessage(e2);
        }

        return '';
    }

    function getHistoricalExpenseLine(description) {
        var candidates = getHistoricalSearchCandidates(description);

        for (var i = 0; i < candidates.length; i++) {
            var candidate = cleanValue(candidates[i]);

            if (!candidate || candidate.length < 3) {
                continue;
            }

            var result = searchHistoricalTransactionLine(candidate);

            if (result && result.accountId) {
                return result;
            }
        }

        return null;
    }

    function getHistoricalSearchCandidates(description) {
        var candidates = [];
        var cleanDescription = cleanValue(description);
        var merchantName = cleanMerchantName(cleanDescription);

        addCandidate(candidates, merchantName);
        addCandidate(candidates, cleanDescription);

        if (cleanDescription.indexOf('*') !== -1) {
            var beforeStar = cleanValue(cleanDescription.substring(0, cleanDescription.indexOf('*')));
            var afterStar = cleanValue(cleanDescription.substring(cleanDescription.indexOf('*') + 1));

            addCandidate(candidates, cleanMerchantName(beforeStar));
            addCandidate(candidates, cleanMerchantName(afterStar));
        }

        var words = merchantName.split(' ');

        for (var i = 0; i < words.length; i++) {
            var word = cleanValue(words[i]);

            if (word && word.length >= 4) {
                addCandidate(candidates, word);
            }
        }

        candidates.sort(function (a, b) {
            return b.length - a.length;
        });

        return candidates;
    }

    function searchHistoricalTransactionLine(searchText) {
        var headerAccountIds = getHeaderAccountIdList();

        var tranSearch = search.create({
            type: search.Type.TRANSACTION,
            filters: [
                ['mainline', 'is', 'F'],
                'AND',
                ['type', 'anyof', 'CardChrg', 'CardRfnd'],
                'AND',
                ['account', 'noneof', headerAccountIds],
                'AND',
                [
                    ['memo', 'contains', searchText],
                    'OR',
                    ['memomain', 'contains', searchText]
                ]
            ],
            columns: [
                search.createColumn({
                    name: 'trandate',
                    sort: search.Sort.DESC
                }),
                search.createColumn({ name: 'internalid' }),
                search.createColumn({ name: 'tranid' }),
                search.createColumn({ name: 'account' }),
                search.createColumn({ name: 'class' })
            ]
        });

        var results = tranSearch.run().getRange({
            start: 0,
            end: 1
        });

        if (!results || results.length === 0) {
            return null;
        }

        return {
            transactionId: results[0].getValue({ name: 'internalid' }),
            accountId: results[0].getValue({ name: 'account' }),
            classId: results[0].getValue({ name: 'class' }) || ''
        };
    }

    function getHeaderAccountIdList() {
        var ids = [];
        var seen = {};

        for (var card in CARD_ACCOUNT_MAP) {
            if (CARD_ACCOUNT_MAP.hasOwnProperty(card)) {
                var id = String(CARD_ACCOUNT_MAP[card]);

                if (!seen[id]) {
                    seen[id] = true;
                    ids.push(id);
                }
            }
        }

        return ids;
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
                search.createColumn({ name: 'name' })
            ]
        });

        fileSearch.run().each(function (result) {
            files.push({
                id: result.getValue({ name: 'internalid' }),
                name: result.getValue({ name: 'name' })
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
            throw 'Header line not found in transaction file.';
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
                rawObj[normalizeHeader(headers[c])] = cols[c] || '';
            }

            data.push({
                SourceFileId: sourceFile.id,
                SourceFileName: sourceFile.name,
                LineNo: r + 1,
                Card: getColumn(rawObj, ['card']),
                TransactionDate: getColumn(rawObj, ['transactiondate']),
                PostDate: getColumn(rawObj, ['postdate']),
                Description: getColumn(rawObj, ['description']),
                Amount: getColumn(rawObj, ['amount'])
            });
        }

        return data;
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
            columns: [
                search.createColumn({ name: 'internalid' })
            ]
        });

        var result = periodSearch.run().getRange({
            start: 0,
            end: 1
        });

        if (result && result.length > 0) {
            return result[0].getValue({ name: 'internalid' });
        }

        throw 'Posting period not found for date.';
    }

    function createErrorFile(fileData) {
        var csv = '';
        csv += 'Line No,Tran ID,Card,Transaction Date,Post Date,Description,Amount,Error Message\n';

        for (var i = 0; i < fileData.errorRows.length; i++) {
            var row = fileData.errorRows[i];

            csv += csvEscape(row.LineNo) + ',';
            csv += csvEscape(row.TranIdNumber) + ',';
            csv += csvEscape(row.Card) + ',';
            csv += csvEscape(row.TransactionDate) + ',';
            csv += csvEscape(row.PostDate) + ',';
            csv += csvEscape(row.Description) + ',';
            csv += csvEscape(row.Amount) + ',';
            csv += csvEscape(row.ErrorMessage) + '\n';
        }

        var errorFile = file.create({
            name: 'credit_card_import_error_' + fileData.fileId + '_' + getDateTimeStamp() + '.csv',
            fileType: file.Type.CSV,
            contents: csv,
            folder: ERROR_FOLDER_ID
        });

        errorFile.save();
    }

    function moveFileToFolder(fileId, folderId) {
        try {
            var inputFile = file.load({ id: fileId });
            inputFile.folder = folderId;
            inputFile.save();
        } catch (e) {
            log.error('Unable To Move File', {
                fileId: fileId,
                folderId: folderId,
                error: getErrorMessage(e)
            });
        }
    }

    function isIgnoredDescription(description) {
        var desc = cleanValue(description).toLowerCase();

        for (var i = 0; i < IGNORE_DESCRIPTION_LIST.length; i++) {
            var ignoreText = cleanValue(IGNORE_DESCRIPTION_LIST[i]).toLowerCase();

            if (desc === ignoreText || desc.indexOf(ignoreText) !== -1) {
                return true;
            }
        }

        return false;
    }

    function cleanMerchantName(value) {
        var text = cleanValue(value);

        if (!text) {
            return '';
        }

        text = text.replace(/&amp;/g, '&');
        text = text.replace(/\s+-\s+.*$/g, '');
        text = text.replace(/\b(TRIP|PAYGO|REBILL|SUBSCRIPTION|PAYMENT|THANK YOU|THANKS|ONLINE|WEB|MOBILE|PURCHASE)\b/gi, '');
        text = text.replace(/[#\/\\]/g, ' ');
        text = text.replace(/\./g, ' ');
        text = text.replace(/\*/g, ' ');
        text = text.replace(/\s+/g, ' ');
        text = text.replace(/\s+[0-9]{3,}$/g, '');
        text = text.replace(/\s+[A-Z]{1,3}\s+[0-9]{3,}$/g, '');
        text = text.replace(/\s+(NY|NJ|CA|TX|FL|IL|TRI)$/i, '');
        text = text.replace(/&[A-Z]$/i, '');

        return cleanValue(text);
    }

    function addCandidate(candidates, value) {
        var text = cleanValue(value);

        if (!text) {
            return;
        }

        for (var i = 0; i < candidates.length; i++) {
            if (normalizeText(candidates[i]) === normalizeText(text)) {
                return;
            }
        }

        candidates.push(text);
    }

    function normalizeText(value) {
        return cleanValue(value)
            .toLowerCase()
            .replace(/&/g, 'and')
            .replace(/[^a-z0-9]/g, '');
    }

    function parseDelimitedLine(line, delimiter) {
        var result = [];
        var current = '';
        var inQuotes = false;

        for (var i = 0; i < line.length; i++) {
            var ch = line.charAt(i);

            if (ch === '"') {
                if (inQuotes && line.charAt(i + 1) === '"') {
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
        return cleanValue(value).toLowerCase().replace(/\s+/g, '');
    }

    function cleanValue(value) {
        if (value === null || value === undefined) {
            return '';
        }

        return String(value)
            .replace(/&amp;/g, '&')
            .replace(/^\s+|\s+$/g, '')
            .replace(/\s+/g, ' ');
    }

    function parseSignedAmount(value) {
        var text = cleanValue(value);

        text = text.replace(/\$/g, '');
        text = text.replace(/,/g, '');

        if (text.charAt(0) === '(' && text.charAt(text.length - 1) === ')') {
            text = '-' + text.substring(1, text.length - 1);
        }

        var amount = parseFloat(text);

        if (isNaN(amount)) {
            throw 'Invalid amount: ' + value;
        }

        return amount;
    }

    function parseDate(value) {
        var text = cleanValue(value);
        var parts = text.split('/');

        if (parts.length !== 3) {
            throw 'Invalid date format. Expected MM/DD/YYYY. Value: ' + value;
        }

        var month = parseInt(parts[0], 10);
        var day = parseInt(parts[1], 10);
        var year = parseInt(parts[2], 10);

        return new Date(year, month - 1, day);
    }

    function csvEscape(value) {
        if (value === null || value === undefined) {
            return '""';
        }

        var text = String(value);
        text = text.replace(/"/g, '""');

        return '"' + text + '"';
    }

    function getDateTimeStamp() {
        var d = new Date();

        return d.getFullYear() +
            pad2(d.getMonth() + 1) +
            pad2(d.getDate()) + '_' +
            pad2(d.getHours()) +
            pad2(d.getMinutes()) +
            pad2(d.getSeconds());
    }

    function pad2(value) {
        return value < 10 ? '0' + value : String(value);
    }

    function getErrorMessage(e) {
        if (!e) {
            return '';
        }

        if (typeof e === 'string') {
            return e;
        }

        if (e.message) {
            return e.message;
        }

        return JSON.stringify(e);
    }

    return {
        getInputData: getInputData,
        map: map,
        summarize: summarize
    };
});