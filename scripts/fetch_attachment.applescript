-- fetch_attachment.applescript
--
-- Tells Apple Mail to find messages in the inbox of the account
-- michael.bask@med.usc.edu whose subject contains the Lab Order
-- Department daily report keyword and which arrived in the last 24
-- hours.  For each match, any .xls / .xlsx attachment is saved into
-- the OneDrive drop folder listed below, and the message is marked
-- as read so a subsequent run does not re-process it.
--
-- The script is driven from scripts/email_ingest.py via:
--     osascript scripts/fetch_attachment.applescript
--
-- Requirements on first run:
--   * Mail.app must be signed in to michael.bask@med.usc.edu
--   * The launchd agent running osascript needs Automation consent
--     to control "Mail" (System Settings -> Privacy & Security ->
--     Automation).  macOS will prompt the first time.
--
-- stdout is written as "LINE: <text>" so the Python caller can log it.

on run
    set targetUser to "michael.bask@med.usc.edu"
    set subjectKeyword to "Lab Order Department Volume Analysis- All Labs Daily Report"
    set destFolder to "/Users/michaelbask/Library/CloudStorage/OneDrive-KeckMedicineofUSC/Work/Productivity Heat Maps/xls_ingest"

    -- Make sure the destination exists before we try to save into it.
    do shell script "mkdir -p " & quoted form of destFolder

    set savedCount to 0
    set matchedCount to 0
    set cutoffDate to ((current date) - (24 * hours))

    tell application "Mail"
        -- Ensure Mail is running so AppleScript can talk to it.
        if not running then
            launch
            delay 2
        end if

        -- Locate the right account by its user name (full UPN / email).
        set targetAccount to missing value
        repeat with acc in accounts
            try
                if (user name of acc) is targetUser then
                    set targetAccount to acc
                    exit repeat
                end if
            end try
        end repeat

        -- Fallback: match by account name containing the local part.
        if targetAccount is missing value then
            repeat with acc in accounts
                try
                    if (name of acc) contains "michael.bask" then
                        set targetAccount to acc
                        exit repeat
                    end if
                end try
            end repeat
        end if

        if targetAccount is missing value then
            log "LINE: ERROR no Mail account found matching " & targetUser
            return "no-account"
        end if

        log "LINE: using account " & (name of targetAccount)

        -- Collect messages across every inbox mailbox belonging to
        -- that account (Exchange accounts can expose more than one).
        set candidateMessages to {}
        repeat with mb in (every mailbox of targetAccount)
            try
                set mbName to name of mb
                if mbName is "INBOX" or mbName is "Inbox" or mbName is "inbox" then
                    set theseMsgs to (messages of mb whose (date received is greater than cutoffDate) and (subject contains subjectKeyword))
                    repeat with m in theseMsgs
                        set end of candidateMessages to m
                    end repeat
                end if
            end try
        end repeat

        set matchedCount to count of candidateMessages
        log "LINE: matched " & matchedCount & " message(s) in last 24h"

        repeat with theMessage in candidateMessages
            set savedForThis to 0
            try
                set msgSubject to subject of theMessage
            on error
                set msgSubject to "(no subject)"
            end try
            log "LINE: inspecting message: " & msgSubject

            try
                set atts to mail attachments of theMessage
            on error
                set atts to {}
            end try

            repeat with att in atts
                try
                    set attName to name of att
                on error
                    set attName to ""
                end try
                if attName is not "" then
                    set lowerName to do shell script "echo " & quoted form of attName & " | tr '[:upper:]' '[:lower:]'"
                    if (lowerName ends with ".xls") or (lowerName ends with ".xlsx") then
                        set savePath to destFolder & "/" & attName
                        try
                            -- Remove any prior copy with the same name so save succeeds.
                            do shell script "rm -f " & quoted form of savePath
                            save att in (POSIX file savePath)
                            set savedForThis to savedForThis + 1
                            set savedCount to savedCount + 1
                            log "LINE: saved " & savePath
                        on error saveErr
                            log "LINE: ERROR saving " & attName & ": " & saveErr
                        end try
                    end if
                end if
            end repeat

            if savedForThis > 0 then
                try
                    set read status of theMessage to true
                    log "LINE: marked message as read"
                on error markErr
                    log "LINE: ERROR marking read: " & markErr
                end try
            end if
        end repeat
    end tell

    log "LINE: done. matched=" & matchedCount & " saved=" & savedCount
    return "matched=" & matchedCount & " saved=" & savedCount
end run
