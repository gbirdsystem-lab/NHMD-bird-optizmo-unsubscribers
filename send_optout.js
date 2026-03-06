/**
 * OptOut Email Sender
 * -------------------
 * Reads collected emails from collected_emails.txt,
 * sends each new (unsent) email to the OptOut API,
 * and tracks sent emails to avoid duplicates.
 *
 * Usage: node send_optout.js
 */

import dotenv from "dotenv";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ─── Configuration ────────────────────────────────────────────────────────────
const API_URL = "https://collect.optoutsystem.com/client-optout/collect";
const API_KEY = process.env.OPTOUT_API_KEY;
const OPTOUT_LIST_ID = process.env.OPTOUT_LIST_ID;

const COLLECTED_EMAILS_FILE = path.join(__dirname, "collected_emails.txt");
const SENT_EMAILS_LOG = path.join(__dirname, "sent_emails.json");

// Delay between API calls (ms) to avoid rate limiting
const DELAY_BETWEEN_REQUESTS_MS = 500;

// ─── Logger ───────────────────────────────────────────────────────────────────
function log(level, message) {
    const timestamp = new Date().toISOString();
    const prefix = {
        INFO: "ℹ️  INFO",
        SUCCESS: "✅ SUCCESS",
        WARN: "⚠️  WARN",
        ERROR: "❌ ERROR",
        API: "🌐 API",
        SEPARATOR: "────────────────────────────────────────",
    }[level] || level;

    if (level === "SEPARATOR") {
        console.log(`[${timestamp}] ${prefix}`);
    } else {
        console.log(`[${timestamp}] ${prefix}: ${message}`);
    }
}

// ─── Sent emails tracking ─────────────────────────────────────────────────────
export function loadSentEmails() {
    if (!fs.existsSync(SENT_EMAILS_LOG)) return new Set();
    try {
        const data = JSON.parse(fs.readFileSync(SENT_EMAILS_LOG, "utf8"));
        return new Set(Array.isArray(data) ? data : []);
    } catch {
        log("WARN", "Could not parse sent_emails.json – starting fresh.");
        return new Set();
    }
}

export function saveSentEmails(sentSet) {
    fs.writeFileSync(
        SENT_EMAILS_LOG,
        JSON.stringify([...sentSet], null, 2),
        "utf8"
    );
}

// ─── Load collected emails ────────────────────────────────────────────────────
function loadCollectedEmails() {
    if (!fs.existsSync(COLLECTED_EMAILS_FILE)) {
        log("WARN", `File not found: ${COLLECTED_EMAILS_FILE}`);
        log("WARN", "Run sftp_reader.js first to collect emails from FTP.");
        return [];
    }
    const content = fs.readFileSync(COLLECTED_EMAILS_FILE, "utf8");
    return content
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line.length > 0);
}

// ─── Send email to API ────────────────────────────────────────────────────────
export async function sendEmailToApi(email) {
    const response = await fetch(API_URL, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${API_KEY}`,
        },
        body: JSON.stringify({
            optoutListId: OPTOUT_LIST_ID,
            email: email,
        }),
    });

    const responseText = await response.text();

    if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${responseText}`);
    }

    return { status: response.status, body: responseText };
}

// ─── Delay helper ─────────────────────────────────────────────────────────────
export function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─── Validate config ──────────────────────────────────────────────────────────
export function validateOptoutConfig() {
    const missing = [];
    if (!API_KEY) missing.push("OPTOUT_API_KEY");
    if (!OPTOUT_LIST_ID) missing.push("OPTOUT_LIST_ID");
    if (missing.length > 0) {
        log("ERROR", `Missing required env variables: ${missing.join(", ")}`);
        log("ERROR", "Add them to your .env file.");
        process.exit(1);
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
    log("SEPARATOR");
    log("INFO", "OptOut Email Sender started");
    log("INFO", `API endpoint : ${API_URL}`);
    log("INFO", `OptOut List  : ${OPTOUT_LIST_ID}`);
    log("SEPARATOR");

    validateOptoutConfig();

    // Load data
    const allEmails = loadCollectedEmails();
    const sentEmails = loadSentEmails();

    log("INFO", `Total collected emails : ${allEmails.length}`);
    log("INFO", `Already sent emails    : ${sentEmails.size}`);

    // Filter to only unsent emails
    const newEmails = allEmails.filter((email) => !sentEmails.has(email));

    if (newEmails.length === 0) {
        log("INFO", "No new emails to send. All emails have already been submitted.");
        log("SEPARATOR");
        return;
    }

    log("SUCCESS", `${newEmails.length} new email(s) to send`);
    log("SEPARATOR");

    // Send each email
    let successCount = 0;
    let failCount = 0;

    for (let i = 0; i < newEmails.length; i++) {
        const email = newEmails[i];
        const progress = `[${i + 1}/${newEmails.length}]`;

        try {
            log("API", `${progress} Sending: ${email}`);
            const result = await sendEmailToApi(email);
            log("SUCCESS", `${progress} Sent: ${email} (HTTP ${result.status})`);

            sentEmails.add(email);
            successCount++;

            // Save after each successful send (so we don't lose progress on crash)
            saveSentEmails(sentEmails);
        } catch (err) {
            log("ERROR", `${progress} Failed to send "${email}": ${err.message}`);
            failCount++;
        }

        // Delay between requests to avoid rate limiting
        if (i < newEmails.length - 1) {
            await delay(DELAY_BETWEEN_REQUESTS_MS);
        }
    }

    log("SEPARATOR");
    log("SUCCESS", `Done! Sent: ${successCount}, Failed: ${failCount}, Total: ${newEmails.length}`);
    log("INFO", `Sent emails log saved to: ${SENT_EMAILS_LOG}`);
    log("SEPARATOR");
}

const currentFile = fileURLToPath(import.meta.url);
if (process.argv[1] && path.resolve(process.argv[1]).toLowerCase() === path.resolve(currentFile).toLowerCase()) {
    main();
}
