Telegram Bulk Quiz Sender Bot (MVP Version)

A fast, simple Telegram bot that converts teacher text into poll quizzes and sends them to selected class groups — in the exact same order as written.

Supports:

Bulk questions

#MSG and #Q blocks

Quiz mode with correct answers

Explanations

Multi-group sending

Throttling + retry safety

Both Owner + Teachers can send quizzes

Auto-detect bulk paste (no commands required)



---

🚀 Features

✔ Paste-only workflow (no command needed)

Teachers can directly paste:

#MSG
Maths

#Q1
2+2 = ?
A) 3
B) 4
C) 5
#ANS: B
#EXP: Explanation text...

#MSG
Maths Completed

Bot automatically:

1. Detects the format


2. Parses everything


3. Shows preview


4. Lets teacher select target groups


5. Sends all polls/messages in correct order




---

👑 Owner Features

Can add/remove teacher-level users (Sudo)

Can add “global chats” visible to all teachers

Can also send quizzes like teachers

Full access to chat management



---

👨‍🏫 Teacher Features

Add their own class groups

Paste bulk questions

Preview & confirm send

Send to multiple chats at once

View only their own chats (privacy)



---

📦 Project Structure

bot-mvp/
├─ .env.example
├─ requirements.txt
├─ app.py
├─ parser.py
├─ sender.py
├─ db.py
├─ README.md
└─ tests/
   └─ test_parser.py


---

📁 File Description

app.py

Main Telegram bot logic (commands + bulk detection + preview + group selection + sending).

parser.py

Core parser: converts teacher text → ordered actions (MSG, POLL).

sender.py

Sequential sending engine with:

2-second throttle

Retry (3 times)

Abort on failure


db.py

SQLite database setup using SQLAlchemy.

.env.example

Template for environment variables.

tests/test_parser.py

Unit tests for the parser.


---

⚙️ Setup Instructions

1️⃣ Install Python packages

pip install -r requirements.txt

2️⃣ Create .env

Copy .env.example → .env

Fill:

BOT_TOKEN=your_bot_token_here
OWNER_ID=123456789
DATABASE_URL=sqlite:///bot.db
THROTTLE_SECONDS=2

3️⃣ Run the bot

python app.py


---

🎯 How to Use the Bot

Step 1 — /start

Owner and teacher see different menus.

Step 2 — Add a class group

Send a message like:

Class9:-100123456789

Owner groups = global
Teacher groups = private to that teacher

Step 3 — Paste bulk text

The bot will reply:

⏳ Analyzing your text...

Then:

Parsed: 20 items — 18 polls, 2 messages.
Preview (first 3)...

Step 4 — Select chats

Inline menu appears:

[Class9] [Physics10] [ScienceBatch]
[🧪 Test Send] [✅ Confirm Send] [❌ Cancel]

Step 5 — Confirm

Bot will start sending:

🚀 Sending started — Job #42.

Step 6 — Completion

🎉 Done! Job #42 completed.
Delivered: 50 items. Failures: 0.


---

🧠 Bulk Format Rules

#MSG block

Normal text message.

#Q block

Poll question + options:

#Q
Question?
A) Option1
B) Option2
#ANS: B
#EXP: Explanation here

Rules:

At least 2 options required

Explanation requires #ANS

If explanation exists without answer → bot shows error

Order always preserved



---

🔥 Error Handling

Bot will stop you before sending if:

Missing options

Invalid ANSWER (e.g., #ANS: Z)

Explanation without ANSWER

No blocks detected


Example:

❌ Parse Error: Explanation provided but no ANSWER in block #3.


---

🧪 Testing Parser

Run:

pytest tests/test_parser
