/**
 * BackUpBrain Pipeline - resilient Google Docs version
 *
 * Important changes:
 * - Uses a character limit, not a word limit. Google Docs supports up to
 *   approximately 1.02 million characters; 750,000 leaves working headroom.
 * - Retries transient Drive/Docs failures with exponential backoff.
 * - Pauses when the active master is inaccessible rather than risking a duplicate write.
 * - Uses a script lock so overlapping triggers cannot edit the master at once.
 * - Verifies an ingestion marker after save; never attempts unsafe rollback.
 * - Pauses on uncertain saves rather than risking duplicate imports.
 */

/***********************
 * CONFIG
 ***********************/
const MEET_RECORDINGS_FOLDER_ID = '0B0Z2gg_OGBu3fjBUcEpnOHQ3dGJpb09mMVJYSW0wMGo0SjA4WWhCdC05bjZ3TXlGTHhGMjA';
const PROCESSED_FOLDER_ID = '1zVL0c5SMkPtBVSu7rOKdWrrC7CjPTjWY';
const MASTER_DOCS_FOLDER_ID = '1zthIYtW_qeqdrWA0r_Kqinlq0I77XmM-';

const SOURCE_LABEL_NAME = '_GEM MEETING NOTES';
const TARGET_LABEL_NAME = '_GEM MEETING NOTES/Logged to LLM';
const REQUIRED_DOC_TITLE_TEXT = 'notes by gemini';

// Google Docs has a 1.02-million-character limit. Keep substantial headroom.
const MAX_CHARS_PER_MASTER_DOC = 500000; // Conservative threshold to reduce Docs write failures.
const MAX_SINGLE_NOTE_CHARS = 650000;
const TEXT_PARAGRAPH_CHUNK_SIZE = 40000;

const MASTER_DOC_BASE_NAME = 'BackUpBrain Master Notes';
const ACTIVE_MASTER_DOC_ID_PROPERTY = 'ACTIVE_MASTER_DOC_ID';
const MASTER_DOC_INDEX_PROPERTY = 'MASTER_DOC_INDEX';
const ACTIVE_MASTER_CHAR_COUNT_PROPERTY = 'ACTIVE_MASTER_CHAR_COUNT';

const SOURCE_DOC_ID_DESCRIPTION_PREFIX = 'BACKUPBRAIN_SOURCE_DOC_ID=';
const INGESTION_MARKER_PREFIX = 'BACKUPBRAIN_INGESTED_FILE_ID=';
const PROCESSED_SOURCE_DOC_IDS_PROPERTY = 'PROCESSED_SOURCE_DOC_IDS_JSON';
const INGESTED_FILE_IDS_PROPERTY = 'INGESTED_FILE_IDS_JSON';

const RETRY_ATTEMPTS = 5;
const RETRY_BASE_DELAY_MS = 1000;
const PIPELINE_LOCK_WAIT_MS = 30000;


/***********************
 * MAIN PIPELINE
 ***********************/
function runGeminiToBackUpBrainPipeline() {
  const lock = LockService.getScriptLock();

  if (!lock.tryLock(PIPELINE_LOCK_WAIT_MS)) {
    throw new Error('Another BackUpBrain run is still active. Try again after it finishes.');
  }

  try {
    Logger.log('=== START: Gemini Notes -> BackUpBrain ===');

    // Validate all configured folders before changing Gmail labels or copying files.
    getFolderOrThrow_(MEET_RECORDINGS_FOLDER_ID, 'Meet Recordings');
    getFolderOrThrow_(PROCESSED_FOLDER_ID, 'Logged Meets');
    getFolderOrThrow_(MASTER_DOCS_FOLDER_ID, 'Master Docs');

    const copySummary = moveGeminiNotesDocsToMeetRecordings_();
    const ingestSummary = runBackUpBrainIngestion_();

    Logger.log('=== PIPELINE COMPLETE ===');
    Logger.log('Copy Summary: ' + JSON.stringify(copySummary));
    Logger.log('Ingestion Summary: ' + JSON.stringify(ingestSummary));

    return { copySummary: copySummary, ingestSummary: ingestSummary };
  } finally {
    lock.releaseLock();
  }
}


/***********************
 * STEP 1: COPY GEMINI DOC LINKS FROM GMAIL
 ***********************/
function moveGeminiNotesDocsToMeetRecordings_() {
  const sourceLabel = GmailApp.getUserLabelByName(SOURCE_LABEL_NAME);
  if (!sourceLabel) {
    throw new Error('Source Gmail label not found: ' + SOURCE_LABEL_NAME);
  }

  const targetLabel = GmailApp.getUserLabelByName(TARGET_LABEL_NAME) ||
    GmailApp.createLabel(TARGET_LABEL_NAME);
  const meetFolder = getFolderOrThrow_(MEET_RECORDINGS_FOLDER_ID, 'Meet Recordings');
  const processedFolder = getFolderOrThrow_(PROCESSED_FOLDER_ID, 'Logged Meets');
  const threads = sourceLabel.getThreads();
  const processedSourceDocIds = getJsonObjectProperty_(PROCESSED_SOURCE_DOC_IDS_PROPERTY);
  const seenThisRun = {};

  const summary = {
    threadsFound: threads.length,
    threadsProcessed: 0,
    docsCopied: 0,
    skippedAlreadyLoggedThread: 0,
    skippedDuplicateDoc: 0,
    skippedNoMatchingDocs: 0,
    copyErrors: 0
  };

  for (let i = 0; i < threads.length; i++) {
    const thread = threads[i];

    if (threadHasLabel_(thread, targetLabel)) {
      summary.skippedAlreadyLoggedThread++;
      continue;
    }

    const messages = thread.getMessages();
    let handledAnything = false;

    for (let j = 0; j < messages.length; j++) {
      const links = extractGoogleDocLinks_(messages[j].getBody());

      for (let k = 0; k < links.length; k++) {
        const sourceDocId = extractGoogleDocId_(links[k]);
        if (!sourceDocId || seenThisRun[sourceDocId]) {
          if (sourceDocId) summary.skippedDuplicateDoc++;
          continue;
        }
        seenThisRun[sourceDocId] = true;

        try {
          const sourceFile = retry_('access source Doc ' + sourceDocId, function () {
            return DriveApp.getFileById(sourceDocId);
          });
          const sourceTitle = retry_('read source Doc metadata ' + sourceDocId, function () {
            return sourceFile.getName();
          });

          if (!sourceTitle ||
              sourceTitle.toLowerCase().indexOf(REQUIRED_DOC_TITLE_TEXT) === -1) {
            continue;
          }

          if (sourceFile.getMimeType() !== MimeType.GOOGLE_DOCS) {
            Logger.log('Skipping link that is not a Google Doc: ' + sourceTitle);
            continue;
          }

          if (meetingNoteAlreadyExists_(
              sourceDocId,
              sourceTitle,
              [meetFolder, processedFolder],
              processedSourceDocIds)) {
            processedSourceDocIds[sourceDocId] = true;
            summary.skippedDuplicateDoc++;
            handledAnything = true;
            Logger.log('Skipped duplicate Gemini note: ' + sourceTitle);
            continue;
          }

          const copiedFile = retry_('copy source Doc ' + sourceDocId, function () {
            return sourceFile.makeCopy(sourceTitle, meetFolder);
          });
          retry_('tag copied Doc ' + copiedFile.getId(), function () {
            copiedFile.setDescription(SOURCE_DOC_ID_DESCRIPTION_PREFIX + sourceDocId);
            return true;
          });

          processedSourceDocIds[sourceDocId] = true;
          summary.docsCopied++;
          handledAnything = true;
          Logger.log('Copied: ' + sourceTitle + ' | copy ID: ' + copiedFile.getId());
        } catch (err) {
          summary.copyErrors++;
          Logger.log('Failed to copy ' + links[k] + ' | ' + formatError_(err));
        }
      }
    }

    if (handledAnything) {
      thread.removeLabel(sourceLabel);
      thread.addLabel(targetLabel);
      summary.threadsProcessed++;
    } else {
      summary.skippedNoMatchingDocs++;
    }
  }

  setJsonObjectProperty_(PROCESSED_SOURCE_DOC_IDS_PROPERTY, processedSourceDocIds);
  return summary;
}


/***********************
 * STEP 2: INGEST FILES
 ***********************/
function runBackUpBrainIngestion_() {
  const newNotesFolder = getFolderOrThrow_(MEET_RECORDINGS_FOLDER_ID, 'Meet Recordings');
  const processedFolder = getFolderOrThrow_(PROCESSED_FOLDER_ID, 'Logged Meets');
  const files = newNotesFolder.getFiles();
  const ingestedFileIds = getJsonObjectProperty_(INGESTED_FILE_IDS_PROPERTY);
  const seenThisRun = {};
  const summary = { filesProcessed: 0, filesSkipped: 0, duplicateFilesMoved: 0, ingestionErrors: 0 };

  while (files.hasNext()) {
    const file = files.next();
    let fileName = '(name unavailable)';
    try {
      const fileId = file.getId();
      fileName = file.getName();
      const sourceDocId = getSourceDocIdFromCopiedFile_(file);
      const duplicateKey = sourceDocId || normalizeFileName_(fileName);

      // Only move an already-ingested file if the persistent registry confirms it.
      // A same-name file is not proof that it was successfully appended.
      if (ingestedFileIds[fileId]) {
        retry_('move previously ingested file ' + fileName, function () {
          file.moveTo(processedFolder);
          return true;
        });
        summary.filesSkipped++;
        summary.duplicateFilesMoved++;
        continue;
      }
      if (seenThisRun[duplicateKey]) {
        Logger.log('Deferred same-run duplicate (left in source folder): ' + fileName);
        summary.filesSkipped++;
        continue;
      }
      seenThisRun[duplicateKey] = true;

      const content = getFileTextSafely_(file);
      if (!content || !content.trim()) {
        summary.filesSkipped++;
        Logger.log('Skipped empty file: ' + fileName);
        continue;
      }
      if (content.length > MAX_SINGLE_NOTE_CHARS) {
        throw new Error('Note exceeds safe per-note limit: ' + content.length + ' characters.');
      }

      const marker = INGESTION_MARKER_PREFIX + fileId;
      const importedOn = new Date().toISOString();
      appendMeetingNoteSafely_(fileName, importedOn, sourceDocId, content, marker);

      // Append function returns only after a fresh open verifies the commit marker.
      ingestedFileIds[fileId] = true;
      setJsonObjectProperty_(INGESTED_FILE_IDS_PROPERTY, ingestedFileIds);
      if (sourceDocId) {
        const processedIds = getJsonObjectProperty_(PROCESSED_SOURCE_DOC_IDS_PROPERTY);
        processedIds[sourceDocId] = true;
        setJsonObjectProperty_(PROCESSED_SOURCE_DOC_IDS_PROPERTY, processedIds);
      }
      retry_('move processed note ' + fileName, function () {
        file.moveTo(processedFolder);
        return true;
      });
      summary.filesProcessed++;
      Logger.log('Processed and verified: ' + fileName);
    } catch (err) {
      summary.ingestionErrors++;
      Logger.log('Error processing file: ' + fileName + ' | ' + formatError_(err));
      try { Logger.log('File URL: ' + file.getUrl()); } catch (ignored) {}
    }
  }
  setJsonObjectProperty_(INGESTED_FILE_IDS_PROPERTY, ingestedFileIds);
  return summary;
}


/***********************
 * SAFE MASTER-DOC APPEND
 ***********************/
function appendMeetingNoteSafely_(fileName, importedOn, sourceDocId, content, marker) {
  const entryCharCount = estimateEntryCharCount_(fileName, importedOn, sourceDocId, content, marker);
  let state = getWritableMasterDoc_();
  if (state.charCount + entryCharCount > MAX_CHARS_PER_MASTER_DOC) {
    closeQuietly_(state.doc);
    state = createNextMasterDoc_();
  }

  // The marker is the commit record. Never remove the last document paragraph.
  // If saving throws, check the actual document before deciding whether to retry.
  {
    const docId = state.id;
    let doc = state.doc;
    try {
      const body = doc.getBody();
      if (body.getText().indexOf(marker) === -1) {
        body.appendHorizontalRule();
        body.appendParagraph('Meeting Note: ' + fileName)
          .setHeading(DocumentApp.ParagraphHeading.HEADING2);
        body.appendParagraph('Imported on: ' + importedOn);
        if (sourceDocId) body.appendParagraph('Source Doc ID: ' + sourceDocId);
        appendTextInChunks_(body, content);
        body.appendParagraph(marker);
      }
      doc.saveAndClose();
      if (!verifyIngestionMarker_(docId, marker)) {
        throw new Error('Save returned but marker could not be verified in master ' + docId);
      }
      const verified = openDocumentWithRetry_(docId, 'verify master size');
      const actualLength = verified.getBody().getText().length;
      closeQuietly_(verified);
      PropertiesService.getScriptProperties()
        .setProperty(ACTIVE_MASTER_CHAR_COUNT_PROPERTY, String(actualLength));
      return;
    } catch (err) {
      Logger.log('Master append failed (' + docId + '): ' + formatError_(err));
      // Do not attempt a destructive rollback: the server may already have saved.
      // Verify first, since a failed save response does not prove failed persistence.
      if (verifyIngestionMarkerBestEffort_(docId, marker)) {
        Logger.log('Append was actually committed; marker verified: ' + marker);
        return;
      }
      // Fail closed. Never retry a write if its outcome cannot be established.
      throw new Error('Uncertain save state in master ' + docId +
        '. Note retained in Meet Recordings. Check for marker ' + marker +
        ' before rerunning. ' + formatError_(err));
    }
  }
}

function verifyIngestionMarker_(docId, marker) {
  const doc = openDocumentWithRetry_(docId, 'verify ingestion marker');
  try {
    return doc.getBody().getText().indexOf(marker) !== -1;
  } finally {
    closeQuietly_(doc);
  }
}

function verifyIngestionMarkerBestEffort_(docId, marker) {
  try { return verifyIngestionMarker_(docId, marker); }
  catch (err) {
    Logger.log('Unable to verify commit on ' + docId + ': ' + formatError_(err));
    return false;
  }
}

function appendTextInChunks_(body, text) {
  for (let offset = 0; offset < text.length; offset += TEXT_PARAGRAPH_CHUNK_SIZE) {
    body.appendParagraph(text.substring(offset, offset + TEXT_PARAGRAPH_CHUNK_SIZE));
  }
}


function estimateEntryCharCount_(fileName, importedOn, sourceDocId, content, marker) {
  return content.length + fileName.length + importedOn.length + marker.length +
    (sourceDocId ? sourceDocId.length : 0) + 200;
}


/***********************
 * MASTER DOC MANAGEMENT
 ***********************/
function getWritableMasterDoc_() {
  const props = PropertiesService.getScriptProperties();
  const activeId = props.getProperty(ACTIVE_MASTER_DOC_ID_PROPERTY);

  if (activeId) {
    try {
      const doc = openDocumentWithRetry_(activeId, 'active master Doc');
      const index = parseInt(props.getProperty(MASTER_DOC_INDEX_PROPERTY), 10) || 1;
      const charCount = doc.getBody().getText().length;
      props.setProperty(ACTIVE_MASTER_CHAR_COUNT_PROPERTY, String(charCount));

      if (charCount >= MAX_CHARS_PER_MASTER_DOC) {
        closeQuietly_(doc);
        return createNextMasterDoc_();
      }

      return { doc: doc, id: activeId, index: index, charCount: charCount };
    } catch (err) {
      Logger.log('Stored active master is inaccessible after retries: ' + activeId);
      Logger.log(formatError_(err));
      // Fail closed: do not silently create a new master when the current one
      // may have a partially committed write. Retry on the next scheduled run.
      throw new Error('Active master is inaccessible; processing paused to avoid duplicates: ' + formatError_(err));
    }
  }

  return selectHighestAccessibleMasterOrCreate_();
}


function selectHighestAccessibleMasterOrCreate_() {
  const candidates = findMasterDocsInFolder_();
  if (!candidates.length) return createMasterDocWithIndex_(1, true);

  const highest = candidates[0];
  try {
    const doc = openDocumentWithRetry_(highest.id, 'highest master Doc');
    const charCount = doc.getBody().getText().length;

    if (charCount >= MAX_CHARS_PER_MASTER_DOC) {
      closeQuietly_(doc);
      return createMasterDocWithIndex_(highest.index + 1, true);
    }

    setActiveMasterProperties_(highest.id, highest.index, charCount);
    return { doc: doc, id: highest.id, index: highest.index, charCount: charCount };
  } catch (err) {
    throw new Error('Highest master inaccessible; processing paused: ' + formatError_(err));
  }
}


function findMasterDocsInFolder_() {
  const folder = getFolderOrThrow_(MASTER_DOCS_FOLDER_ID, 'Master Docs');
  const files = folder.getFiles();
  const candidates = [];

  while (files.hasNext()) {
    try {
      const file = files.next();
      const name = file.getName();
      if (file.getMimeType() !== MimeType.GOOGLE_DOCS) continue;

      const index = getMasterDocIndexFromName_(name);
      if (index === null) continue;

      candidates.push({ id: file.getId(), name: name, index: index, url: file.getUrl() });
    } catch (err) {
      Logger.log('Skipped inaccessible item in Master Docs folder: ' + formatError_(err));
    }
  }

  candidates.sort(function (a, b) { return b.index - a.index; });
  return candidates;
}


function getMasterDocIndexFromName_(name) {
  if (!name) return null;
  const escaped = escapeRegExp_(MASTER_DOC_BASE_NAME);
  const match = String(name).trim().match(new RegExp('^' + escaped + '(?:\\s+(\\d+)|\\s*\\((\\d+)\\))?$', 'i'));
  if (!match) return null;
  return parseInt(match[1] || match[2] || '1', 10);
}


function createNextMasterDoc_() {
  const candidates = findMasterDocsInFolder_();
  const nextIndex = candidates.length ? candidates[0].index + 1 : 1;
  return createMasterDocWithIndex_(nextIndex, true);
}


function createMasterDocWithIndex_(index, sendNotification) {
  const name = MASTER_DOC_BASE_NAME + ' ' + index;
  // Do not retry creation itself: if Google created the file but only the response
  // failed, retrying could create two masters with the same number.
  const doc = DocumentApp.create(name);
  const id = doc.getId();

  try {
    const body = doc.getBody();
    body.appendParagraph(name).setHeading(DocumentApp.ParagraphHeading.TITLE);
    body.appendParagraph('Created on: ' + new Date().toLocaleString());
    body.appendParagraph('');
    body.appendParagraph('NotebookLM Source Doc');
    body.appendParagraph('Add this Google Doc as a source in your BackUpBrain NotebookLM notebook.');
    doc.saveAndClose();

    const folder = getFolderOrThrow_(MASTER_DOCS_FOLDER_ID, 'Master Docs');
    const file = retry_('access newly created master Doc ' + id, function () {
      return DriveApp.getFileById(id);
    });
    retry_('move new master Doc into Master Docs folder', function () {
      file.moveTo(folder);
      return true;
    });

    const verifiedDoc = openDocumentWithRetry_(id, 'new master Doc');
    const charCount = verifiedDoc.getBody().getText().length;
    setActiveMasterProperties_(id, index, charCount);

    Logger.log('Created new active master: ' + name + ' | ' + file.getUrl());

    if (sendNotification) {
      try {
        notifyMeToAddNewMasterDocToNotebookLM_(name, file.getUrl(), index);
      } catch (emailErr) {
        Logger.log('Master was created, but reminder email failed: ' + formatError_(emailErr));
      }
    }

    return { doc: verifiedDoc, id: id, index: index, charCount: charCount };
  } catch (err) {
    closeQuietly_(doc);
    throw new Error('Created master Doc ' + id + ' but could not initialize it: ' + formatError_(err));
  }
}


function setActiveMasterProperties_(id, index, charCount) {
  PropertiesService.getScriptProperties().setProperties({
    ACTIVE_MASTER_DOC_ID: id,
    MASTER_DOC_INDEX: String(index),
    ACTIVE_MASTER_CHAR_COUNT: String(charCount)
  });
}


function openDocumentWithRetry_(docId, context) {
  return retry_('open ' + context + ' (' + docId + ')', function () {
    const file = DriveApp.getFileById(docId);
    if (file.getMimeType() !== MimeType.GOOGLE_DOCS) {
      throw new Error('File is not a Google Doc; MIME type is ' + file.getMimeType());
    }
    return DocumentApp.openById(docId);
  });
}


/***********************
 * FILE READING
 ***********************/
function getFileTextSafely_(file) {
  const fileId = file.getId();
  const mimeType = file.getMimeType();

  if (mimeType === MimeType.PLAIN_TEXT) {
    return retry_('read text file ' + fileId, function () {
      return file.getBlob().getDataAsString();
    });
  }
  if (mimeType === MimeType.GOOGLE_DOCS) {
    return exportGoogleDocTextWithRetry_(fileId);
  }
  throw new Error('Unsupported MIME type: ' + mimeType);
}


function exportGoogleDocTextWithRetry_(fileId) {
  return retry_('export Google Doc ' + fileId, function () {
    const url = 'https://www.googleapis.com/drive/v3/files/' +
      encodeURIComponent(fileId) + '/export?mimeType=text/plain';
    const response = UrlFetchApp.fetch(url, {
      method: 'get',
      headers: { Authorization: 'Bearer ' + ScriptApp.getOAuthToken() },
      muteHttpExceptions: true
    });
    const code = response.getResponseCode();

    if (code !== 200) {
      throw new Error('Drive export HTTP ' + code + ': ' +
        response.getContentText().substring(0, 500));
    }
    return response.getContentText();
  });
}


/***********************
 * DUPLICATE HELPERS
 ***********************/
function meetingNoteAlreadyExists_(sourceDocId, sourceTitle, folders, registry) {
  if (registry[sourceDocId]) return true;
  const wantedName = normalizeFileName_(sourceTitle);

  for (let i = 0; i < folders.length; i++) {
    const files = folders[i].getFiles();
    while (files.hasNext()) {
      try {
        const file = files.next();
        if (getSourceDocIdFromCopiedFile_(file) === sourceDocId ||
            normalizeFileName_(file.getName()) === wantedName) {
          return true;
        }
      } catch (err) {
        Logger.log('Skipped inaccessible file during duplicate scan: ' + formatError_(err));
      }
    }
  }
  return false;
}


function normalizeFileName_(name) {
  return name ? String(name).toLowerCase().replace(/\s+/g, ' ')
    .replace(/\s*\(\d+\)\s*$/g, '').trim() : '';
}


function getSourceDocIdFromCopiedFile_(file) {
  try {
    const description = file.getDescription() || '';
    return description.indexOf(SOURCE_DOC_ID_DESCRIPTION_PREFIX) === 0 ?
      description.substring(SOURCE_DOC_ID_DESCRIPTION_PREFIX.length).trim() : null;
  } catch (err) {
    return null;
  }
}


/***********************
 * GMAIL / LINK HELPERS
 ***********************/
function extractGoogleDocLinks_(html) {
  if (!html) return [];
  const matches = html.match(/https:\/\/docs\.google\.com\/document\/d\/[a-zA-Z0-9_-]+\/?[^\s"'<>)]*/g) || [];
  const unique = [];
  const seen = {};

  for (let i = 0; i < matches.length; i++) {
    const id = extractGoogleDocId_(matches[i]);
    if (id && !seen[id]) {
      seen[id] = true;
      unique.push(matches[i]);
    }
  }
  return unique;
}


function extractGoogleDocId_(url) {
  const match = String(url || '').match(/\/document\/d\/([a-zA-Z0-9_-]+)/);
  return match ? match[1] : null;
}


function threadHasLabel_(thread, label) {
  const labels = thread.getLabels();
  for (let i = 0; i < labels.length; i++) {
    if (labels[i].getName() === label.getName()) return true;
  }
  return false;
}


/***********************
 * GENERAL HELPERS
 ***********************/
function retry_(operationName, operation) {
  let lastError;

  for (let attempt = 1; attempt <= RETRY_ATTEMPTS; attempt++) {
    try {
      return operation();
    } catch (err) {
      lastError = err;
      if (attempt === RETRY_ATTEMPTS) break;

      const delay = RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1);
      Logger.log(operationName + ' failed (attempt ' + attempt + '/' + RETRY_ATTEMPTS +
        '): ' + formatError_(err) + '. Retrying in ' + delay + ' ms.');
      Utilities.sleep(delay);
    }
  }

  throw new Error(operationName + ' failed after ' + RETRY_ATTEMPTS +
    ' attempts. Last error: ' + formatError_(lastError));
}


function getFolderOrThrow_(folderId, label) {
  try {
    return retry_('open ' + label + ' folder', function () {
      const folder = DriveApp.getFolderById(folderId);
      folder.getName(); // Force an access check; Drive objects can otherwise be lazy.
      return folder;
    });
  } catch (err) {
    throw new Error(label + ' folder is inaccessible. Confirm the ID, that it is a folder, and that the ' +
      'trigger owner has access. ID: ' + folderId + ' | ' + formatError_(err));
  }
}


function closeQuietly_(doc) {
  if (!doc) return;
  try { doc.saveAndClose(); } catch (ignored) {}
}


function formatError_(err) {
  if (!err) return 'Unknown error';
  return err.message || String(err);
}


function escapeRegExp_(text) {
  return String(text).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}


function getJsonObjectProperty_(propertyName) {
  const raw = PropertiesService.getScriptProperties().getProperty(propertyName);
  if (!raw) return {};
  try { return JSON.parse(raw); } catch (err) {
    throw new Error('Invalid registry property ' + propertyName + '; refusing to reset processing history: ' + formatError_(err));
  }
}


function setJsonObjectProperty_(propertyName, objectValue) {
  PropertiesService.getScriptProperties()
    .setProperty(propertyName, JSON.stringify(objectValue));
}


/***********************
 * NOTEBOOKLM NOTIFICATION
 ***********************/
function notifyMeToAddNewMasterDocToNotebookLM_(docName, docUrl, index) {
  const email = Session.getEffectiveUser().getEmail();
  if (!email) {
    Logger.log('No effective-user email available; skipping NotebookLM reminder.');
    return;
  }

  GmailApp.sendEmail(
    email,
    'New BackUpBrain Master Doc Created: ' + docName,
    'A new BackUpBrain master source Doc was created.\n\n' +
    'Document Name:\n' + docName + '\n\n' +
    'Document Link:\n' + docUrl + '\n\n' +
    'Add this Google Doc as a source in your BackUpBrain NotebookLM notebook.\n\n' +
    'Master Doc Number:\n' + index
  );
}


/***********************
 * MANUAL DIAGNOSTIC / RECOVERY TOOLS
 ***********************/
function diagnoseBackUpBrainAccess() {
  const result = {
    folders: {},
    activeMaster: {},
    masterCandidates: []
  };

  const folders = {
    meetRecordings: MEET_RECORDINGS_FOLDER_ID,
    processed: PROCESSED_FOLDER_ID,
    masters: MASTER_DOCS_FOLDER_ID
  };

  Object.keys(folders).forEach(function (key) {
    try {
      const folder = DriveApp.getFolderById(folders[key]);
      result.folders[key] = { ok: true, name: folder.getName(), id: folders[key] };
    } catch (err) {
      result.folders[key] = { ok: false, id: folders[key], error: formatError_(err) };
    }
  });

  const props = PropertiesService.getScriptProperties();
  const activeId = props.getProperty(ACTIVE_MASTER_DOC_ID_PROPERTY);
  result.activeMaster.id = activeId;

  if (activeId) {
    try {
      const doc = openDocumentWithRetry_(activeId, 'diagnostic active master');
      result.activeMaster.ok = true;
      result.activeMaster.name = doc.getName();
      result.activeMaster.characters = doc.getBody().getText().length;
      closeQuietly_(doc);
    } catch (err) {
      result.activeMaster.ok = false;
      result.activeMaster.error = formatError_(err);
    }
  }

  try {
    result.masterCandidates = findMasterDocsInFolder_();
  } catch (err) {
    result.masterScanError = formatError_(err);
  }

  Logger.log(JSON.stringify(result, null, 2));
  return result;
}


function forceCreateRecoveryMaster() {
  const lock = LockService.getScriptLock();
  lock.waitLock(PIPELINE_LOCK_WAIT_MS);
  try {
    const state = createNextMasterDoc_();
    const result = { id: state.id, index: state.index, name: state.doc.getName() };
    closeQuietly_(state.doc);
    Logger.log(JSON.stringify(result));
    return result;
  } finally {
    lock.releaseLock();
  }
}


// Deliberately no bulk property-reset function: clearing the processed registry
// can lead to duplicate copies and repeated meeting notes.

function rebuildDuplicateRegistryFromDriveFolders() {
  const registry = {};
  addSourceDocIdsFromFolderToRegistry_(
    getFolderOrThrow_(PROCESSED_FOLDER_ID, 'Logged Meets'), registry);
  addSourceDocIdsFromFolderToRegistry_(
    getFolderOrThrow_(MEET_RECORDINGS_FOLDER_ID, 'Meet Recordings'), registry);
  setJsonObjectProperty_(PROCESSED_SOURCE_DOC_IDS_PROPERTY, registry);
  Logger.log('Rebuilt duplicate registry with ' + Object.keys(registry).length + ' source IDs.');
}


function addSourceDocIdsFromFolderToRegistry_(folder, registry) {
  const files = folder.getFiles();
  while (files.hasNext()) {
    const sourceDocId = getSourceDocIdFromCopiedFile_(files.next());
    if (sourceDocId) registry[sourceDocId] = true;
  }
}
