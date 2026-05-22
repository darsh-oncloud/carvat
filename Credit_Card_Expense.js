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

    var BACKEND_MAPPING_RECORD_TYPE = 'customrecord_credit_card_backend_mapping';
    var MAPPING_CARD_FIELD_ID = 'custrecord_card_number';
    var MAPPING_CATEGORY_FIELD_ID = 'custrecord_category';
    var MAPPING_EXPENSE_ACCOUNT_FIELD_ID = 'custrecord_account_number';
    var MAPPING_EMPLOYEE_NAME_FIELD_ID = 'custrecord_employee_name';

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
            pendingFileCount: pendingFiles.length,
            startingTranId: nextTranId
        });

        if (!pendingFiles || pendingFiles.length === 0) {
            return [];
        }

        for (var i = 0; i < pendingFiles.length; i++) {
            try {
                var inputFile = file.load({ id: pendingFiles[i].id });
                var rows = parseTransactionFile(inputFile.getContents(), pendingFiles[i]);

                for (var r = 0; r < rows.length; r++) {
                    if (isIgnoredDescription(rows[r].Description)) {
                        rows[r].IsSkippedLine = true;
                        rows[r].TranIdNumber = '';
                        rows[r].InputError = 'Skipped description, transaction not created: ' + rows[r].Description;
                    } else {
                        rows[r].TranIdNumber = nextTranId;
                        nextTranId = incrementBigNumberString(nextTranId);
                    }

                    allRows.push(rows[r]);
                }

            } catch (e) {
                allRows.push({
                    SourceFileId: pendingFiles[i].id,
                    SourceFileName: pendingFiles[i].name,
                    LineNo: '',
                    Card: '',
                    TransactionDate: '',
                    PostDate: '',
                    Description: '',
                    Category: '',
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
            log.audit('ROW PROCESS STARTED', row);

            if (row.InputError) {
                throw row.InputError;
            }

            var cardNo = cleanValue(row.Card);
            var description = cleanValue(row.Description);
            var category = cleanValue(row.Category);
            var postDateText = cleanValue(row.PostDate);
            var rawAmountText = cleanValue(row.Amount);

            if (!cardNo) throw 'Missing Card Number';
            if (!description) throw 'Missing Description';
            if (!category) throw 'Missing Category';
            if (!postDateText) throw 'Missing Post Date';
            if (!rawAmountText) throw 'Missing Amount';

            var creditCardAccountId = CARD_ACCOUNT_MAP[cardNo];

            if (!creditCardAccountId) {
                throw 'No header account mapping found for card number: ' + cardNo;
            }

            var rawAmount = parseSignedAmount(rawAmountText);
            var amountPositive = Math.abs(rawAmount);
            var recordType = rawAmount < 0 ? 'creditcardcharge' : 'creditcardrefund';

            var postDate = parseDate(postDateText);
            var postingPeriodId = getPostingPeriod(postDate);

            var entityInfo = getOtherNameEntityInfoFromDescription(description);
            var entityId = entityInfo.id;

            var expenseLine = getExpenseLineFromBackendMapping(cardNo, category);
            var historicalLine = getHistoricalExpenseLine(description);

            var employeeName = '';

            if (expenseLine && expenseLine.employeeName) {
                employeeName = expenseLine.employeeName;
            }

            if (!expenseLine || !expenseLine.accountId) {
                expenseLine = historicalLine;
            }

            if (!expenseLine || !expenseLine.accountId) {
                throw 'No expense account found from custom mapping or history. Card: ' + cardNo + ' | Category: ' + category + ' | Description: ' + description;
            }

            if (historicalLine && historicalLine.classId) {
                expenseLine.classId = historicalLine.classId;
            }

            if (!expenseLine.classId) {
                throw 'Class is blank in historical transaction for description: ' + description;
            }

            var finalMemo = buildMainMemo(employeeName, description);

            log.debug('FINAL VALUES BEFORE CREATE', {
                cardNo: cardNo,
                category: category,
                employeeName: employeeName,
                finalMemo: finalMemo,
                headerAccount: creditCardAccountId,
                expenseAccount: expenseLine.accountId,
                classId: expenseLine.classId,
                amount: amountPositive,
                recordType: recordType
            });

            var ccRec = record.create({
                type: recordType,
                isDynamic: true
            });

            ccRec.setValue({ fieldId: 'tranid', value: String(row.TranIdNumber) });
            ccRec.setValue({ fieldId: 'entity', value: entityId });
            ccRec.setValue({ fieldId: 'account', value: creditCardAccountId });
            ccRec.setValue({ fieldId: 'usertotal', value: amountPositive });
            ccRec.setValue({ fieldId: 'trandate', value: postDate });
            ccRec.setValue({ fieldId: 'postingperiod', value: postingPeriodId });
            ccRec.setValue({ fieldId: 'memo', value: finalMemo });
            ccRec.setValue({ fieldId: 'class', value: expenseLine.classId });

            ccRec.selectNewLine({ sublistId: 'expense' });

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

            ccRec.setCurrentSublistValue({
                sublistId: 'expense',
                fieldId: 'class',
                value: expenseLine.classId
            });

            ccRec.commitLine({ sublistId: 'expense' });

            var recId = ccRec.save({
                enableSourcing: true,
                ignoreMandatoryFields: false
            });

            log.audit('TRANSACTION CREATED SUCCESSFULLY', {
                tranid: row.TranIdNumber,
                recordId: recId,
                recordType: recordType
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

            log.error('CREDIT CARD ROW FAILED', {
                line: row.LineNo,
                tranid: row.TranIdNumber,
                row: row,
                error: e
            });

            context.write({
                key: 'ERROR',
                value: JSON.stringify(row)
            });
        }
    }

    function summarize(summary) {
        var fileStatus = {};
        var totalCreated = 0;
        var totalSkipped = 0;
        var totalErrors = 0;

        summary.output.iterator().each(function (key, value) {
            var obj = JSON.parse(value);
            var fileId = obj.SourceFileId;

            if (!fileId) return true;

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
                totalCreated++;
            }

            if (key === 'ERROR') {
                fileStatus[fileId].errorRows.push(obj);

                if (obj.IsSkippedLine === true || obj.IsSkippedLine === 'true') {
                    totalSkipped++;
                } else {
                    totalErrors++;
                }
            }

            return true;
        });

        log.audit('CREDIT CARD IMPORT SUMMARY', {
            totalCreated: totalCreated,
            totalSkipped: totalSkipped,
            totalErrors: totalErrors
        });

        for (var fileId in fileStatus) {
            if (fileStatus.hasOwnProperty(fileId)) {
                if (fileStatus[fileId].errorRows.length > 0) {
                    createErrorFile(fileStatus[fileId]);
                }

                moveFileToFolder(fileId, PROCESSED_FOLDER_ID);
            }
        }
    }

    function getExpenseLineFromBackendMapping(cardNo, category) {
        var cardInternalId = getCardMappingInternalId(cardNo);
        var categoryInternalId = getCategoryInternalId(category);

        log.debug('CARD / CATEGORY INTERNAL IDS', {
            cardNo: cardNo,
            cardInternalId: cardInternalId,
            category: category,
            categoryInternalId: categoryInternalId
        });

        if (!cardInternalId || !categoryInternalId) {
            return null;
        }

        var mappingSearch = search.create({
            type: BACKEND_MAPPING_RECORD_TYPE,
            filters: [
                [MAPPING_CARD_FIELD_ID, 'anyof', cardInternalId],
                'AND',
                [MAPPING_CATEGORY_FIELD_ID, 'anyof', categoryInternalId]
            ],
            columns: [
                search.createColumn({ name: MAPPING_EXPENSE_ACCOUNT_FIELD_ID }),
                search.createColumn({ name: MAPPING_EMPLOYEE_NAME_FIELD_ID })
            ]
        });

        var results = mappingSearch.run().getRange({
            start: 0,
            end: 1
        });

        log.debug('BACKEND MAPPING RESULT COUNT', {
            cardNo: cardNo,
            category: category,
            count: results ? results.length : 0
        });

        if (!results || results.length === 0) {
            return null;
        }

        return {
            accountId: results[0].getValue({ name: MAPPING_EXPENSE_ACCOUNT_FIELD_ID }),
            employeeName: results[0].getText({ name: MAPPING_EMPLOYEE_NAME_FIELD_ID }) ||
                results[0].getValue({ name: MAPPING_EMPLOYEE_NAME_FIELD_ID }) ||
                '',
            classId: ''
        };
    }

    function getCardMappingInternalId(cardNo) {
        var cardSearch = search.create({
            type: BACKEND_MAPPING_RECORD_TYPE,
            filters: [
                [MAPPING_CARD_FIELD_ID, 'noneof', '@NONE@']
            ],
            columns: [
                search.createColumn({ name: MAPPING_CARD_FIELD_ID })
            ]
        });

        var results = cardSearch.run().getRange({
            start: 0,
            end: 1000
        });

        for (var i = 0; results && i < results.length; i++) {
            var cardText = results[i].getText({ name: MAPPING_CARD_FIELD_ID });
            var cardValue = results[i].getValue({ name: MAPPING_CARD_FIELD_ID });

            if (
                cleanValue(cardText) === cleanValue(cardNo) ||
                cleanValue(cardValue) === cleanValue(cardNo)
            ) {
                return cardValue;
            }
        }

        return '';
    }

    function getCategoryInternalId(category) {
        var categorySearch = search.create({
            type: BACKEND_MAPPING_RECORD_TYPE,
            filters: [
                [MAPPING_CATEGORY_FIELD_ID, 'noneof', '@NONE@']
            ],
            columns: [
                search.createColumn({ name: MAPPING_CATEGORY_FIELD_ID })
            ]
        });

        var results = categorySearch.run().getRange({
            start: 0,
            end: 1000
        });

        for (var i = 0; results && i < results.length; i++) {
            var categoryText = results[i].getText({ name: MAPPING_CATEGORY_FIELD_ID });
            var categoryValue = results[i].getValue({ name: MAPPING_CATEGORY_FIELD_ID });

            if (
                normalizeText(categoryText) === normalizeText(category) ||
                cleanValue(categoryValue) === cleanValue(category)
            ) {
                return categoryValue;
            }
        }

        return '';
    }

    function getNextTranIdNumber() {
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
                search.createColumn({ name: 'internalid', sort: search.Sort.DESC }),
                search.createColumn({ name: 'tranid' })
            ]
        });

        var results = tranSearch.run().getRange({
            start: 0,
            end: 1
        });

        if (results && results.length > 0) {
            var tranIdText = cleanValue(results[0].getValue({ name: 'tranid' }));

            if (isOnlyDigits(tranIdText)) {
                return incrementBigNumberString(tranIdText);
            }
        }

        return String(DEFAULT_TRANID_START_FROM);
    }

    function getHistoricalExpenseLine(description) {
        var candidates = getHistoricalSearchCandidates(description);

        for (var i = 0; i < candidates.length; i++) {
            var candidate = cleanValue(candidates[i]);

            if (!candidate || candidate.length < 3) continue;

            var result = searchHistoricalTransactionLine(candidate);

            if (result && result.accountId) {
                return result;
            }
        }

        return null;
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
                search.createColumn({ name: 'trandate', sort: search.Sort.DESC }),
                search.createColumn({ name: 'account' }),
                search.createColumn({ name: 'class' })
            ]
        });

        var results = tranSearch.run().getRange({
            start: 0,
            end: 10
        });

        if (!results || results.length === 0) {
            return null;
        }

        return {
            accountId: results[0].getValue({ name: 'account' }),
            classId: results[0].getValue({ name: 'class' }) || ''
        };
    }

    function getOtherNameEntityInfoFromDescription(description) {
        var entityInfo = findEntityFromHistory(description);

        if (entityInfo && entityInfo.id) {
            return entityInfo;
        }

        var cleaned = cleanMerchantName(description);
        var result = findOtherNameByName(cleaned);

        if (result && result.id) {
            return result;
        }

        var words = cleanValue(description).toUpperCase().split(/\s+/).map(function (w) {
            return w.replace(/[^A-Z0-9]/g, '');
        }).filter(function (w) {
            return w.length >= 4 && !/^[0-9]+$/.test(w);
        });

        for (var i = 0; i < words.length; i++) {
            result = findOtherNameByName(words[i]);

            if (result && result.id) {
                return result;
            }
        }

        throw 'Other Name not found. New Other Name will not be created. Description: ' + description;
    }

    function findEntityFromHistory(description) {
        var candidates = getHistoricalSearchCandidates(description);

        for (var i = 0; i < candidates.length; i++) {
            var candidate = cleanValue(candidates[i]);

            if (!candidate || candidate.length < 3) continue;

            var tranSearch = search.create({
                type: search.Type.TRANSACTION,
                filters: [
                    ['type', 'anyof', 'CardChrg', 'CardRfnd'],
                    'AND',
                    ['mainline', 'is', 'T'],
                    'AND',
                    [
                        ['memo', 'contains', candidate],
                        'OR',
                        ['memomain', 'contains', candidate]
                    ]
                ],
                columns: [
                    search.createColumn({ name: 'trandate', sort: search.Sort.DESC }),
                    search.createColumn({ name: 'entity' })
                ]
            });

            var results = tranSearch.run().getRange({
                start: 0,
                end: 1
            });

            if (results && results.length > 0) {
                var entityId = results[0].getValue({ name: 'entity' });
                var entityName = results[0].getText({ name: 'entity' });

                if (entityId) {
                    return {
                        id: entityId,
                        name: entityName
                    };
                }
            }
        }

        return null;
    }

    function findOtherNameByName(otherNameName) {
        var text = cleanValue(otherNameName);

        if (!text || text.length < 3) return null;

        var entitySearch = search.create({
            type: 'othername',
            filters: [
                ['isinactive', 'is', 'F'],
                'AND',
                ['entityid', 'contains', text]
            ],
            columns: [
                search.createColumn({ name: 'internalid' }),
                search.createColumn({ name: 'entityid' })
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
            id: results[0].getValue({ name: 'internalid' }),
            name: results[0].getValue({ name: 'entityid' })
        };
    }

    function buildMainMemo(employeeName, description) {
        employeeName = cleanValue(employeeName);
        description = cleanValue(description);

        if (employeeName && description) {
            return employeeName + ' - ' + description;
        }

        if (description) {
            return description;
        }

        return employeeName || '';
    }

    function getPendingFiles() {
        var files = [];

        var fileSearch = search.create({
            type: 'file',
            filters: [
                ['folder', 'anyof', PENDING_FOLDER_ID]
            ],
            columns: [
                search.createColumn({ name: 'internalid', sort: search.Sort.ASC }),
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
            if (lines[i] && lines[i].indexOf('Card') !== -1 && lines[i].indexOf('Amount') !== -1) {
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
            if (!lines[r] || !cleanValue(lines[r])) continue;

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
                Category: getColumn(rawObj, ['category']),
                Amount: getColumn(rawObj, ['amount'])
            });
        }

        return data;
    }

    function getHistoricalSearchCandidates(description) {
        var candidates = [];
        var cleanDescription = cleanValue(description);
        var merchantName = cleanMerchantName(cleanDescription);

        addCandidate(candidates, merchantName);
        addCandidate(candidates, cleanDescription);

        if (cleanDescription.indexOf('*') !== -1) {
            addCandidate(candidates, cleanMerchantName(cleanDescription.substring(0, cleanDescription.indexOf('*'))));
            addCandidate(candidates, cleanMerchantName(cleanDescription.substring(cleanDescription.indexOf('*') + 1)));
        }

        return candidates;
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

        throw 'Posting period not found for date: ' + dateText;
    }

    function createErrorFile(fileData) {
        var csv = 'Line No,Tran ID,Card,Transaction Date,Post Date,Description,Amount,Error Message\n';

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

        file.create({
            name: 'credit_card_import_error_' + fileData.fileId + '_' + getDateTimeStamp() + '.csv',
            fileType: file.Type.CSV,
            contents: csv,
            folder: ERROR_FOLDER_ID
        }).save();
    }

    function moveFileToFolder(fileId, folderId) {
        try {
            var inputFile = file.load({ id: fileId });
            inputFile.folder = folderId;
            inputFile.save();
        } catch (e) {
            log.error('FILE MOVE FAILED', {
                fileId: fileId,
                folderId: folderId,
                error: e
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
        return cleanValue(value)
            .replace(/&amp;/g, '&')
            .replace(/\s+-\s+.*$/g, '')
            .replace(/\b(TRIP|PAYGO|REBILL|SUBSCRIPTION|PAYMENT|THANK YOU|THANKS|ONLINE|WEB|MOBILE|PURCHASE)\b/gi, '')
            .replace(/[#\/\\]/g, ' ')
            .replace(/\./g, ' ')
            .replace(/\*/g, ' ')
            .replace(/\s+/g, ' ');
    }

    function addCandidate(candidates, value) {
        var text = cleanValue(value);

        if (!text) return;

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

    function isOnlyDigits(value) {
        return /^[0-9]+$/.test(cleanValue(value));
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
        if (value === null || value === undefined) return '';

        return String(value)
            .replace(/&amp;/g, '&')
            .replace(/^\s+|\s+$/g, '')
            .replace(/\s+/g, ' ');
    }

    function parseSignedAmount(value) {
        var text = cleanValue(value).replace(/\$/g, '').replace(/,/g, '');

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

        if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
            var isoParts = text.split('-');

            return new Date(
                parseInt(isoParts[0], 10),
                parseInt(isoParts[1], 10) - 1,
                parseInt(isoParts[2], 10)
            );
        }

        var parts = text.split('/');

        if (parts.length !== 3) {
            throw 'Invalid date format. Expected MM/DD/YYYY or YYYY-MM-DD. Value: ' + value;
        }

        return new Date(
            parseInt(parts[2], 10),
            parseInt(parts[0], 10) - 1,
            parseInt(parts[1], 10)
        );
    }

    function csvEscape(value) {
        var text = value === null || value === undefined ? '' : String(value);
        return '"' + text.replace(/"/g, '""') + '"';
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