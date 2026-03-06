/**
 * SFTP File Reader
 * ----------------
 * Connects to an SFTP server, navigates to a target folder,
 * detects new/unread files, reads their contents, and logs everything.
 *
 * Runs locally (node sftp_reader.js) or via GitHub Actions cron.
 */

import dotenv from "dotenv";
import SftpClient from "ssh2-sftp-client";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { execSync } from "child_process";
import { sendEmailToApi, loadSentEmails, saveSentEmails, delay, validateOptoutConfig } from "./send_optout.js";

// Load .env file (ignored in GitHub Actions – uses Secrets instead)
dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ─── Configuration ────────────────────────────────────────────────────────────
const SFTP_HOST = process.env.SFTP_HOST;
const SFTP_PORT = parseInt(process.env.SFTP_PORT || "22", 10);
const SFTP_USER = process.env.SFTP_USER;
const SFTP_PASSWORD = process.env.SFTP_PASSWORD;
const SFTP_PRIVATE_KEY_PATH = process.env.SFTP_PRIVATE_KEY_PATH || null;
const SFTP_REMOTE_DIR = process.env.SFTP_REMOTE_DIR || "/";

// Only process files with these extensions
const ALLOWED_EXTENSIONS = [".json", ".jsonl"];

/**
 * File that stores the names of already-processed files so we
 * don't re-read them on the next run.
 */
const PROCESSED_FILES_LOG = path.join(__dirname, "processed_files.json");
const COLLECTED_EMAILS_FILE = path.join(__dirname, "collected_emails.txt");

// ─── Runtime limit ────────────────────────────────────────────────────────────
// workflow timeout-minutes: 160  →  give ourselves 10 min of headroom
const MAX_RUNTIME_MS = 150 * 60 * 1000; // 150 minutes
const START_TIME = Date.now();

/**
 * Returns true when fewer than 5 minutes remain before MAX_RUNTIME_MS.
 * Call this at the top of every file-iteration to exit the loop cleanly
 * before GitHub kills the runner mid-operation.
 */
function isTimeRunningOut() {
    const elapsed = Date.now() - START_TIME;
    const remaining = MAX_RUNTIME_MS - elapsed;
    if (remaining < 5 * 60 * 1000) {
        log("WARN", `⏰ Time running out! Elapsed: ${Math.round(elapsed / 60000)} min, remaining: ${Math.round(remaining / 60000)} min. Stopping gracefully…`);
        return true;
    }
    return false;
}

/** Human-readable elapsed time string, e.g. "1h 23m 45s" */
function elapsedTime() {
    const ms = Date.now() - START_TIME;
    const s = Math.floor(ms / 1000) % 60;
    const m = Math.floor(ms / 60000) % 60;
    const h = Math.floor(ms / 3600000);
    return h > 0 ? `${h}h ${m}m ${s}s` : m > 0 ? `${m}m ${s}s` : `${s}s`;
}

// ─── Logger ───────────────────────────────────────────────────────────────────
function log(level, message) {
    const timestamp = new Date().toISOString();
    const prefix = {
        INFO:      "ℹ️  INFO",
        SUCCESS:   "✅ SUCCESS",
        WARN:      "⚠️  WARN",
        ERROR:     "❌ ERROR",
        FILE:      "📄 FILE",
        TIME:      "⏱️  TIME",
        SEPARATOR: "────────────────────────────────────────",
    }[level] || level;

    if (level === "SEPARATOR") {
        console.log(`[${timestamp}] ${prefix}`);
    } else {
        console.log(`[${timestamp}] ${prefix}: ${message}`);
    }
}

// ─── Processed files tracking ─────────────────────────────────────────────────
function loadProcessedFiles() {
    if (!fs.existsSync(PROCESSED_FILES_LOG)) return new Set();
    try {
        const data = JSON.parse(fs.readFileSync(PROCESSED_FILES_LOG, "utf8"));
        return new Set(Array.isArray(data) ? data : []);
    } catch {
        log("WARN", "Could not parse processed_files.json – starting fresh.");
        return new Set();
    }
}

function saveProcessedFiles(processedSet) {
    fs.writeFileSync(
        PROCESSED_FILES_LOG,
        JSON.stringify([...processedSet], null, 2),
        "utf8"
    );
}

// ─── Validate environment ─────────────────────────────────────────────────────
function validateConfig() {
    const missing = [];
    if (!SFTP_HOST) missing.push("SFTP_HOST");
    if (!SFTP_USER)  missing.push("SFTP_USER");
    if (!SFTP_PASSWORD && !SFTP_PRIVATE_KEY_PATH) {
        missing.push("SFTP_PASSWORD or SFTP_PRIVATE_KEY_PATH");
    }
    if (missing.length > 0) {
        log("ERROR", `Missing required env variables: ${missing.join(", ")}`);
        log("ERROR", "Copy .env.example to .env and fill in your credentials.");
        process.exit(1);
    }
    validateOptoutConfig();
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
    log("SEPARATOR");
    log("INFO",  "SFTP File Reader started");
    log("INFO",  `Target host  : ${SFTP_HOST}:${SFTP_PORT}`);
    log("INFO",  `Remote dir   : ${SFTP_REMOTE_DIR}`);
    log("TIME",  `Max runtime  : ${MAX_RUNTIME_MS / 60000} min (stops with 5 min headroom)`);
    log("SEPARATOR");

    validateConfig();

    const processedFiles = loadProcessedFiles();
    const sentEmails     = loadSentEmails();

    log("INFO", `State loaded – already processed files : ${processedFiles.size}`);
    log("INFO", `State loaded – already sent emails     : ${sentEmails.size}`);

    const sftp = new SftpClient();

    // Build connection config
    const connectConfig = {
        host:     SFTP_HOST,
        port:     SFTP_PORT,
        username: SFTP_USER,
    };

    if (SFTP_PRIVATE_KEY_PATH) {
        connectConfig.privateKey = fs.readFileSync(SFTP_PRIVATE_KEY_PATH);
        log("INFO", `Auth method : private key (${SFTP_PRIVATE_KEY_PATH})`);
    } else {
        connectConfig.password = SFTP_PASSWORD;
        log("INFO", "Auth method : password");
    }

    try {
        // ── Connect ──────────────────────────────────────────────────────────────
        log("INFO", "Connecting to SFTP server…");
        await sftp.connect(connectConfig);
        log("SUCCESS", `Connected! [${elapsedTime()}]`);

        // ── List files in target directory ───────────────────────────────────────
        log("INFO", `Listing files in: ${SFTP_REMOTE_DIR}`);
        const fileList = await sftp.list(SFTP_REMOTE_DIR);

        // Filter to regular files only (type '-') and only allowed extensions
        const allFiles    = fileList.filter((item) => item.type === "-");
        const remoteFiles = allFiles.filter((item) =>
            ALLOWED_EXTENSIONS.includes(path.extname(item.name).toLowerCase())
        );
        log("INFO", `Found ${allFiles.length} total file(s), ${remoteFiles.length} JSON/JSONL file(s) in remote directory.`);

        if (remoteFiles.length === 0) {
            log("WARN", "No files found in the remote directory.");
            await runWeeklyCleanup(sftp);
            return;
        }

        // ── Detect new files ─────────────────────────────────────────────────────
        const newFiles = remoteFiles.filter((file) => !processedFiles.has(file.name));

        if (newFiles.length === 0) {
            log("INFO", "No new files detected since last run. Nothing to process.");
            await runWeeklyCleanup(sftp);
            return;
        }

        log("SUCCESS", `${newFiles.length} new file(s) found – processing… [${elapsedTime()}]`);
        log("SEPARATOR");

        // ── Read and process each new file ───────────────────────────────────────
        let successCount   = 0;
        let skippedCount   = 0;
        const collectedEmails = [];

        for (let fileIdx = 0; fileIdx < newFiles.length; fileIdx++) {
            const file = newFiles[fileIdx];

            // ── Graceful early-exit check ─────────────────────────────────────
            if (isTimeRunningOut()) {
                log("WARN", `Stopping after ${fileIdx}/${newFiles.length} files to save state before timeout.`);
                skippedCount = newFiles.length - fileIdx;
                break;
            }

            const remotePath = `${SFTP_REMOTE_DIR.replace(/\/$/, "")}/${file.name}`;
            log("INFO", `[${fileIdx + 1}/${newFiles.length}] Reading: ${file.name}  (${file.size} bytes, modified: ${new Date(file.modifyTime).toISOString()})  [elapsed: ${elapsedTime()}]`);

            const fileEmails = [];

            try {
                // Retrieve file content as a Buffer, then decode as UTF-8 text
                const buffer  = await sftp.get(remotePath);
                const content = buffer.toString("utf8");
                const ext     = path.extname(file.name).toLowerCase();

                log("FILE", `── Contents of "${file.name}" ──────────────`);

                if (ext === ".jsonl") {
                    const lines = content.split(/\r?\n/).filter((l) => l.trim());
                    log("INFO", `File contains ${lines.length} JSONL record(s)`);

                    lines.forEach((line, idx) => {
                        try {
                            const record = JSON.parse(line);
                            log("FILE", `Record #${idx + 1}:`);
                            console.log(JSON.stringify(record, null, 2));
                            if (record.email) {
                                collectedEmails.push(record.email);
                                fileEmails.push(record.email);
                            }
                        } catch {
                            log("WARN", `Record #${idx + 1} is not valid JSON: ${line.substring(0, 200)}`);
                        }
                    });
                } else if (ext === ".json") {
                    try {
                        const parsed  = JSON.parse(content);
                        const summary = Array.isArray(parsed)
                            ? `JSON array with ${parsed.length} element(s)`
                            : `JSON object with keys: ${Object.keys(parsed).join(", ")}`;
                        log("INFO", summary);
                        console.log(JSON.stringify(parsed, null, 2));
                        const items = Array.isArray(parsed) ? parsed : [parsed];
                        for (const item of items) {
                            if (item && item.email) {
                                collectedEmails.push(item.email);
                                fileEmails.push(item.email);
                            }
                        }
                    } catch {
                        log("WARN", `File is not valid JSON. Raw content (first 500 chars): ${content.substring(0, 500)}`);
                    }
                }

                const lineCount = content.split(/\r?\n/).length;
                log("FILE", `── End of "${file.name}" (${lineCount} lines) ──`);

                // ── Send emails to API ────────────────────────────────────────
                let fileApiErrors = 0;
                let newSentCount  = 0;

                for (let i = 0; i < fileEmails.length; i++) {
                    // Check time inside the email loop too – files with thousands
                    // of emails could alone exhaust the remaining runtime.
                    if (isTimeRunningOut()) {
                        log("WARN", `⏰ Stopping email sending mid-file (${i}/${fileEmails.length} sent for ${file.name}).`);
                        fileApiErrors++; // treat as incomplete → keep file on SFTP
                        break;
                    }

                    const email    = fileEmails[i];
                    const progress = `[${i + 1}/${fileEmails.length}]`;

                    if (!sentEmails.has(email)) {
                        try {
                            log("INFO", `${progress} API: Sending ${email}  [elapsed: ${elapsedTime()}]`);
                            const result = await sendEmailToApi(email);
                            log("SUCCESS", `${progress} API: Sent ${email} (HTTP ${result.status})`);

                            sentEmails.add(email);
                            saveSentEmails(sentEmails);
                            newSentCount++;
                        } catch (err) {
                            log("ERROR", `Failed to send "${email}": ${err.message}`);
                            fileApiErrors++;
                        }

                        if (i < fileEmails.length - 1) {
                            await delay(500);
                        }
                    } else {
                        log("INFO", `${progress} API: Skipping ${email} (already sent)`);
                    }
                }

                // ── Delete / mark processed ───────────────────────────────────
                if (fileApiErrors === 0) {
                    log("INFO", `All data for ${file.name} processed successfully (${newSentCount} newly sent). Deleting from SFTP… [${elapsedTime()}]`);
                    try {
                        await sftp.delete(remotePath);
                        log("SUCCESS", `Deleted ${file.name} from SFTP.`);
                    } catch (delErr) {
                        log("ERROR", `Failed to delete ${file.name} from SFTP: ${delErr.message}. Marking as processed anyway.`);
                    }
                    processedFiles.add(file.name);
                    saveProcessedFiles(processedFiles); // ← persist after every file
                    successCount++;
                } else {
                    log("WARN", `Could not mark ${file.name} complete (${fileApiErrors} failed/incomplete API call(s)). Keeping on SFTP for retry next run.`);
                }

            } catch (fileErr) {
                log("ERROR", `Failed to read "${file.name}": ${fileErr.message}`);
            }

            log("SEPARATOR");
        }

        // ── Final state save ─────────────────────────────────────────────────────
        saveProcessedFiles(processedFiles);
        log("SUCCESS", `Done! Processed ${successCount}/${newFiles.length} new file(s). Skipped (time limit): ${skippedCount}. [total elapsed: ${elapsedTime()}]`);
        log("INFO",    `Processed files list saved to: ${PROCESSED_FILES_LOG}`);

        // ── Save collected emails to text file ───────────────────────────────────
        if (collectedEmails.length > 0) {
            let existingEmails = new Set();
            if (fs.existsSync(COLLECTED_EMAILS_FILE)) {
                const existing = fs.readFileSync(COLLECTED_EMAILS_FILE, "utf8");
                existing.split(/\r?\n/).filter((e) => e.trim()).forEach((e) => existingEmails.add(e.trim()));
            }

            let newEmailCount = 0;
            for (const email of collectedEmails) {
                if (!existingEmails.has(email)) {
                    existingEmails.add(email);
                    newEmailCount++;
                }
            }

            const allEmails = [...existingEmails];
            fs.writeFileSync(COLLECTED_EMAILS_FILE, allEmails.join("\n") + "\n", "utf8");
            log("SUCCESS", `Collected ${collectedEmails.length} email(s) total, ${newEmailCount} new. Saved to: ${COLLECTED_EMAILS_FILE}`);
            log("INFO",    `Total unique emails in file: ${allEmails.length}`);
        } else {
            log("INFO", "No emails found in processed files.");
        }

        await runWeeklyCleanup(sftp);

    } catch (err) {
        log("ERROR", `SFTP error: ${err.message}`);
        process.exit(1);
    } finally {
        await sftp.end();
        log("INFO", `SFTP connection closed. [total elapsed: ${elapsedTime()}]`);
        log("SEPARATOR");
    }
}

// ─── Weekly cleanup ───────────────────────────────────────────────────────────
async function runWeeklyCleanup(sftp) {
    const currentDay = new Date().getDay(); // 0 = Sunday

    if (currentDay === 0) {
        log("SEPARATOR");
        log("INFO", "Running weekly security cleanup check (Sunday routine)…");

        const finalFileList  = await sftp.list(SFTP_REMOTE_DIR);
        const danglingFiles  = finalFileList.filter((item) =>
            item.type === "-" && ALLOWED_EXTENSIONS.includes(path.extname(item.name).toLowerCase())
        );

        if (danglingFiles.length === 0) {
            log("INFO", "Verification passed: FTP server is clean. No dangling files remain.");
            log("INFO", "Proceeding to securely empty local tracking files…");

            const SENT_EMAILS_LOG = path.join(__dirname, "sent_emails.json");
            try {
                fs.writeFileSync(PROCESSED_FILES_LOG,  "[]\n", "utf8");
                fs.writeFileSync(COLLECTED_EMAILS_FILE, "",    "utf8");
                fs.writeFileSync(SENT_EMAILS_LOG,       "[]\n", "utf8");
                log("SUCCESS", "Security cleanup successful: tracking files have been emptied.");
            } catch (cleanupErr) {
                log("ERROR", `Failed to empty local tracking files: ${cleanupErr.message}`);
            }
        } else {
            log("WARN", `Verification failed: ${danglingFiles.length} file(s) still on FTP server.`);
            log("WARN", "Skipping local cleanup to avoid losing pending emails.");
        }
    }
}

main();
