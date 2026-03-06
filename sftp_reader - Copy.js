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

// ─── Logger ───────────────────────────────────────────────────────────────────
function log(level, message) {
    const timestamp = new Date().toISOString();
    const prefix = {
        INFO: "ℹ️  INFO",
        SUCCESS: "✅ SUCCESS",
        WARN: "⚠️  WARN",
        ERROR: "❌ ERROR",
        FILE: "📄 FILE",
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
    if (!SFTP_USER) missing.push("SFTP_USER");
    if (!SFTP_PASSWORD && !SFTP_PRIVATE_KEY_PATH) {
        missing.push("SFTP_PASSWORD or SFTP_PRIVATE_KEY_PATH");
    }
    if (missing.length > 0) {
        log("ERROR", `Missing required env variables: ${missing.join(", ")}`);
        log("ERROR", "Copy .env.example to .env and fill in your credentials.");
        process.exit(1);
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
    log("SEPARATOR");
    log("INFO", "SFTP File Reader started");
    log("INFO", `Target host : ${SFTP_HOST}:${SFTP_PORT}`);
    log("INFO", `Remote dir  : ${SFTP_REMOTE_DIR}`);
    log("SEPARATOR");

    validateConfig();

    const processedFiles = loadProcessedFiles();
    const sftp = new SftpClient();

    // Build connection config
    const connectConfig = {
        host: SFTP_HOST,
        port: SFTP_PORT,
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
        log("SUCCESS", "Connected!");

        // ── List files in target directory ───────────────────────────────────────
        log("INFO", `Listing files in: ${SFTP_REMOTE_DIR}`);
        const fileList = await sftp.list(SFTP_REMOTE_DIR);

        // Filter to regular files only (type '-') and only allowed extensions
        const allFiles = fileList.filter((item) => item.type === "-");
        const remoteFiles = allFiles.filter((item) => {
            const ext = path.extname(item.name).toLowerCase();
            return ALLOWED_EXTENSIONS.includes(ext);
        });
        log("INFO", `Found ${allFiles.length} total file(s), ${remoteFiles.length} JSON/JSONL file(s) in remote directory.`);

        if (remoteFiles.length === 0) {
            log("WARN", "No files found in the remote directory.");
            return;
        }

        // ── Detect new files ─────────────────────────────────────────────────────
        const newFiles = remoteFiles.filter(
            (file) => !processedFiles.has(file.name)
        );

        if (newFiles.length === 0) {
            log("INFO", "No new files detected since last run. Nothing to process.");
            return;
        }

        log("SUCCESS", `${newFiles.length} new file(s) found – processing…`);
        log("SEPARATOR");

        // ── Read and log each new file ───────────────────────────────────────────
        let successCount = 0;
        const collectedEmails = [];

        for (const file of newFiles) {
            const remotePath = `${SFTP_REMOTE_DIR.replace(/\/$/, "")}/${file.name}`;
            log("INFO", `Reading file: ${file.name}  (size: ${file.size} bytes, modified: ${new Date(file.modifyTime).toISOString()})`);

            try {
                // Retrieve file content as a Buffer, then decode as UTF-8 text
                const buffer = await sftp.get(remotePath);
                const content = buffer.toString("utf8");
                const ext = path.extname(file.name).toLowerCase();

                log("FILE", `── Contents of "${file.name}" ──────────────`);

                if (ext === ".jsonl") {
                    // JSONL – each line is a separate JSON record
                    const lines = content.split(/\r?\n/).filter((l) => l.trim());
                    log("INFO", `File contains ${lines.length} JSONL record(s)`);

                    lines.forEach((line, idx) => {
                        try {
                            const record = JSON.parse(line);
                            log("FILE", `Record #${idx + 1}:`);
                            console.log(JSON.stringify(record, null, 2));
                            // Collect email if present
                            if (record.email) {
                                collectedEmails.push(record.email);
                            }
                        } catch {
                            log("WARN", `Record #${idx + 1} is not valid JSON: ${line.substring(0, 200)}`);
                        }
                    });
                } else if (ext === ".json") {
                    // JSON – parse entire file as one JSON object/array
                    try {
                        const parsed = JSON.parse(content);
                        const summary = Array.isArray(parsed)
                            ? `JSON array with ${parsed.length} element(s)`
                            : `JSON object with keys: ${Object.keys(parsed).join(", ")}`;
                        log("INFO", summary);
                        console.log(JSON.stringify(parsed, null, 2));
                        // Collect emails from JSON array or single object
                        const items = Array.isArray(parsed) ? parsed : [parsed];
                        for (const item of items) {
                            if (item && item.email) {
                                collectedEmails.push(item.email);
                            }
                        }
                    } catch {
                        log("WARN", `File is not valid JSON. Raw content (first 500 chars): ${content.substring(0, 500)}`);
                    }
                }

                const lineCount = content.split(/\r?\n/).length;
                log("FILE", `── End of "${file.name}" (${lineCount} lines) ──`);

                processedFiles.add(file.name);
                successCount++;
            } catch (fileErr) {
                log("ERROR", `Failed to read "${file.name}": ${fileErr.message}`);
            }

            log("SEPARATOR");
        }

        // ── Save updated processed list ──────────────────────────────────────────
        saveProcessedFiles(processedFiles);
        log("SUCCESS", `Done! Processed ${successCount}/${newFiles.length} new file(s).`);
        log("INFO", `Processed files list saved to: ${PROCESSED_FILES_LOG}`);

        // ── Save collected emails to text file ───────────────────────────────────
        if (collectedEmails.length > 0) {
            // Load existing emails to avoid duplicates
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
            log("INFO", `Total unique emails in file: ${allEmails.length}`);
        } else {
            log("INFO", "No emails found in processed files.");
        }
    } catch (err) {
        log("ERROR", `SFTP error: ${err.message}`);
        process.exit(1);
    } finally {
        await sftp.end();
        log("INFO", "SFTP connection closed.");
        log("SEPARATOR");
    }
}

main();
