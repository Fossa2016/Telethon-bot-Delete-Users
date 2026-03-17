import asyncio
import os
import shlex
import time

import asyncpg
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions.channels import EditBannedRequest
from telethon.tl.types import ChatBannedRights

# ============ КОНФИГ ============

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
DATABASE_URL = os.getenv("DATABASE_URL")
BOT_TOKEN = os.getenv("BOT_TOKEN")
USER_SESSION = os.getenv("USER_SESSION")
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "").split(",")))

# ============ КЛИЕНТЫ ============

user_client = TelegramClient(StringSession(USER_SESSION), API_ID, API_HASH)
bot_client = TelegramClient('bot_session', API_ID, API_HASH)

pool = None


# ============ БАЗА ============

async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)
    
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users(
                chat_id BIGINT,
                user_id BIGINT,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                display_name TEXT,
                PRIMARY KEY(chat_id, user_id)
            )
        """)

        # Если таблица уже существует без новых колонок — добавляем их
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS managers(
                admin_id BIGINT,
                group_id BIGINT,
                group_title TEXT,
                PRIMARY KEY(admin_id, group_id)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS selected_group(
                admin_id BIGINT PRIMARY KEY,
                group_id BIGINT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_kicks(
                id SERIAL PRIMARY KEY,
                chat_id BIGINT,
                user_id BIGINT,
                kick_at DOUBLE PRECISION
            )
        """)


# ============ ПАРСИНГ УЧАСТНИКОВ ============

async def sync_members(chat_id):
    """Получить ВСЕХ участников канала/группы и сохранить в базу"""
    try:
        entity = await user_client.get_entity(chat_id)
        participants = await user_client.get_participants(entity, aggressive=True)

        saved = 0
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM users WHERE chat_id=$1", chat_id)

            for user in participants:
                if user.bot:
                    continue

                display_name = " ".join(filter(None, [user.first_name, user.last_name]))

                await conn.execute("""
                    INSERT INTO users (chat_id, user_id, username, first_name, last_name, display_name)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (chat_id, user_id)
                    DO UPDATE SET username=$3, first_name=$4, last_name=$5, display_name=$6
                """, chat_id, user.id, user.username, user.first_name, user.last_name, display_name)

                saved += 1

        return saved
    except Exception as e:
        print(f"Ошибка синхронизации: {e}")
        return -1


# ============ ПОИСК ПОЛЬЗОВАТЕЛЯ ============

async def find_user(conn, target, group_id):
    """
    Ищет пользователя:
    - @username → по username
    - "Имя Фамилия" или просто Имя → по display_name
    """
    if target.startswith("@"):
        value = target[1:]
        return await conn.fetchrow(
            "SELECT user_id, display_name FROM users WHERE username=$1 AND chat_id=$2 LIMIT 1",
            value, group_id
        )
    else:
        return await conn.fetchrow(
            "SELECT user_id, display_name FROM users WHERE display_name=$1 AND chat_id=$2 LIMIT 1",
            target, group_id
        )


# ============ КОМАНДЫ БОТА ============

@bot_client.on(events.NewMessage(pattern='/start'))
async def start(event):
    if not event.is_private:
        return
    
    if not is_admin(event.sender_id):
        return
    
    await event.reply(
        "Бот для удаления участников каналов/групп.\n\n"
        "Команды:\n"
        "/link <id или @username канала> — привязать канал\n"
        "/groups — список привязанных каналов\n"
        "/select <group_id> — выбрать канал\n"
        "/sync — загрузить участников в базу\n"
        "/list — показать участников из базы\n"
        '/kick @username или /kick "Имя Фамилия" — удалить сразу\n'
        '/add @username 60 или /add "Имя Фамилия" 60 — удалить через N сек\n'
        "/count — количество участников в базе"
    )


@bot_client.on(events.NewMessage(pattern='/link'))
async def link_group(event):
    if not event.is_private:
        return
    
    if not is_admin(event.sender_id):
        return

    parts = event.text.split(maxsplit=1)
    if len(parts) != 2:
        await event.reply("Использование: /link <id или @username канала>")
        return

    target = parts[1].strip()

    try:
        entity = await user_client.get_entity(target)
        chat_id = entity.id

        if hasattr(entity, 'broadcast') or hasattr(entity, 'megagroup'):
            chat_id_full = int(f"-100{chat_id}") if chat_id > 0 else chat_id
        else:
            chat_id_full = chat_id

        title = getattr(entity, 'title', str(chat_id))

        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO managers (admin_id, group_id, group_title)
                VALUES ($1, $2, $3)
                ON CONFLICT (admin_id, group_id)
                DO UPDATE SET group_title=$3
            """, event.sender_id, chat_id_full, title)

        await event.reply(f"Канал **{title}** привязан.\nID: `{chat_id_full}`")

    except Exception as e:
        await event.reply(f"Ошибка: {e}\n\nУбедитесь, что user-аккаунт состоит в этом канале.")


@bot_client.on(events.NewMessage(pattern='/groups'))
async def groups_list(event):
    if not event.is_private:
        return
    
    if not is_admin(event.sender_id):
        return

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT group_id, group_title FROM managers WHERE admin_id=$1",
            event.sender_id
        )

    if not rows:
        await event.reply("Нет привязанных каналов. Используйте /link")
        return

    text = "**Ваши каналы:**\n\n"
    for row in rows:
        text += f"• {row['group_title']} — `{row['group_id']}`\n"

    await event.reply(text)


@bot_client.on(events.NewMessage(pattern='/select'))
async def select_group(event):
    if not event.is_private:
        return
    
    if not is_admin(event.sender_id):
        return

    parts = event.text.split()
    if len(parts) != 2:
        await event.reply("Использование: /select <group_id>")
        return

    try:
        group_id = int(parts[1])
    except ValueError:
        await event.reply("group_id должен быть числом")
        return

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM managers WHERE admin_id=$1 AND group_id=$2",
            event.sender_id, group_id
        )

    if not row:
        await event.reply("Этот канал вам не привязан")
        return

    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO selected_group (admin_id, group_id)
            VALUES ($1, $2)
            ON CONFLICT (admin_id)
            DO UPDATE SET group_id=$2
        """, event.sender_id, group_id)

    await event.reply("Канал выбран ✅")


@bot_client.on(events.NewMessage(pattern='/sync'))
async def sync_command(event):
    if not event.is_private:
        return
    
    if not is_admin(event.sender_id):
        return

    async with pool.acquire() as conn:
        selected = await conn.fetchrow(
            "SELECT group_id FROM selected_group WHERE admin_id=$1",
            event.sender_id
        )

    if not selected:
        await event.reply("Сначала выберите канал через /select")
        return

    group_id = selected["group_id"]
    await event.reply("Синхронизация участников... Это может занять время.")

    count = await sync_members(group_id)

    if count >= 0:
        await event.reply(f"Готово! Загружено **{count}** участников в базу.")
    else:
        await event.reply("Ошибка при синхронизации. Проверьте, что user-аккаунт — админ канала.")


@bot_client.on(events.NewMessage(pattern='/list'))
async def list_users(event):
    if not event.is_private:
        return
    
    if not is_admin(event.sender_id):
        return

    async with pool.acquire() as conn:
        selected = await conn.fetchrow(
            "SELECT group_id FROM selected_group WHERE admin_id=$1",
            event.sender_id
        )

    if not selected:
        await event.reply("Сначала выберите канал через /select")
        return

    group_id = selected["group_id"]

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT username, display_name, user_id FROM users WHERE chat_id=$1 LIMIT 50",
            group_id
        )

    if not rows:
        await event.reply("База пуста. Сначала выполните /sync")
        return

    text = "**Участники (первые 50):**\n\n"
    for row in rows:
        uname = f"@{row['username']}" if row['username'] else "без username"
        name = row['display_name'] or "без имени"
        text += f"• {name} — {uname} — `{row['user_id']}`\n"

    await event.reply(text)


@bot_client.on(events.NewMessage(pattern='/count'))
async def count_users(event):
    if not event.is_private:
        return
    
    if not is_admin(event.sender_id):
        return

    async with pool.acquire() as conn:
        selected = await conn.fetchrow(
            "SELECT group_id FROM selected_group WHERE admin_id=$1",
            event.sender_id
        )

    if not selected:
        await event.reply("Сначала выберите канал через /select")
        return

    group_id = selected["group_id"]

    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE chat_id=$1",
            group_id
        )

    await event.reply(f"В базе **{count}** участников")


@bot_client.on(events.NewMessage(pattern='/kick'))
async def kick_now(event):
    if not event.is_private:
        return
    
    if not is_admin(event.sender_id):
        return

    async with pool.acquire() as conn:
        selected = await conn.fetchrow(
            "SELECT group_id FROM selected_group WHERE admin_id=$1",
            event.sender_id
        )

    if not selected:
        await event.reply("Сначала выберите канал через /select")
        return

    group_id = selected["group_id"]

    try:
        parts = shlex.split(event.text)
    except ValueError:
        await event.reply('Ошибка в кавычках. Пример: /kick @username или /kick "Имя Фамилия"')
        return

    if len(parts) != 2:
        await event.reply('Использование: /kick @username или /kick "Имя Фамилия"')
        return

    target = parts[1].strip()

    async with pool.acquire() as conn:
        row = await find_user(conn, target, group_id)

    if not row:
        await event.reply("Пользователь не найден в базе. Выполните /sync")
        return

    user_id = row["user_id"]
    display = row["display_name"] or target

    try:
        await user_client(EditBannedRequest(
            channel=group_id,
            participant=user_id,
            banned_rights=ChatBannedRights(
                until_date=None,
                view_messages=True
            )
        ))

        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM users WHERE chat_id=$1 AND user_id=$2",
                group_id, user_id
            )

        await event.reply(f"{display} удалён ✅")

    except Exception as e:
        await event.reply(f"Ошибка: {e}")


@bot_client.on(events.NewMessage(pattern='/add'))
async def add_delayed_kick(event):

    if not event.is_private:
        return
    
    if not is_admin(event.sender_id):
        return
    
    async with pool.acquire() as conn:
        selected = await conn.fetchrow(
            "SELECT group_id FROM selected_group WHERE admin_id=$1",
            event.sender_id
        )

    if not selected:
        await event.reply("Сначала выберите канал через /select")
        return

    group_id = selected["group_id"]

    try:
        parts = shlex.split(event.text)
    except ValueError:
        await event.reply('Ошибка в кавычках. Пример: /add @username 60 или /add "Имя Фамилия" 60')
        return

    if len(parts) != 3:
        await event.reply('Использование: /add @username 60 или /add "Имя Фамилия" 60')
        return

    target = parts[1].strip()

    try:
        seconds = int(parts[2])
    except ValueError:
        await event.reply("Второй аргумент должен быть числом (секунды)")
        return

    async with pool.acquire() as conn:
        row = await find_user(conn, target, group_id)

    if not row:
        await event.reply("Пользователь не найден в базе. Выполните /sync")
        return

    user_id = row["user_id"]
    display = row["display_name"] or target
    kick_at = time.time() + seconds

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO pending_kicks (chat_id, user_id, kick_at) VALUES ($1, $2, $3)",
            group_id, user_id, kick_at
        )

    await event.reply(f"{display} будет удалён через {seconds} сек ⏳")

# ============ АВТОДОБАВЛЕНИЕ НОВЫХ УЧАСТНИКОВ ============

@user_client.on(events.ChatAction)
async def on_user_joined(event):
    """Когда кто-то вступает в группу/канал — добавляем в базу"""
    if not event.user_joined and not event.user_added:
        return

    try:
        user = await event.get_user()
        if user is None or user.bot:
            return

        chat = await event.get_chat()
        chat_id = chat.id

        # Приводим к формату -100...
        if hasattr(chat, 'broadcast') or hasattr(chat, 'megagroup'):
            chat_id_full = int(f"-100{chat_id}") if chat_id > 0 else chat_id
        else:
            chat_id_full = chat_id

        display_name = " ".join(filter(None, [user.first_name, user.last_name]))

        async with pool.acquire() as conn:
            # Проверяем, что этот чат вообще отслеживается
            is_managed = await conn.fetchval(
                "SELECT 1 FROM managers WHERE group_id=$1 LIMIT 1",
                chat_id_full
            )
            if not is_managed:
                return

            await conn.execute("""
                INSERT INTO users (chat_id, user_id, username, first_name, last_name, display_name)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (chat_id, user_id)
                DO UPDATE SET username=$3, first_name=$4, last_name=$5, display_name=$6
            """, chat_id_full, user.id, user.username, user.first_name, user.last_name, display_name)

        print(f"Новый участник: {display_name} ({user.id}) в {chat_id_full}")

    except Exception as e:
        print(f"Ошибка автодобавления: {e}")

# ============ ФОНОВЫЙ КИКЕР ============

async def kick_loop():
    while True:
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT id, chat_id, user_id FROM pending_kicks WHERE kick_at <= $1",
                    time.time()
                )

                for row in rows:
                    try:
                        await user_client(EditBannedRequest(
                            channel=row["chat_id"],
                            participant=row["user_id"],
                            banned_rights=ChatBannedRights(
                                until_date=None,
                                view_messages=True
                            )
                        ))
                        print(f"Кикнут {row['user_id']} из {row['chat_id']}")

                        await conn.execute(
                            "DELETE FROM pending_kicks WHERE id=$1",
                            row["id"]
                        )
                        await conn.execute(
                            "DELETE FROM users WHERE chat_id=$1 AND user_id=$2",
                            row["chat_id"], row["user_id"]
                        )

                    except Exception as e:
                        print(f"Ошибка кика {row['user_id']}: {e}")

                    await asyncio.sleep(3)

        except Exception as e:
            print(f"Ошибка в kick_loop: {e}")

        await asyncio.sleep(30)

def is_admin(user_id):
    return user_id in ADMIN_IDS

# ============ ЗАПУСК ============

async def main():
    await init_db()

    await user_client.connect()
    if not await user_client.is_user_authorized():
        raise RuntimeError("USER_SESSION невалидна или отсутствует")
    print("User-client запущен")

    await bot_client.start(bot_token=BOT_TOKEN)
    print("Bot-client запущен")

    asyncio.create_task(kick_loop())

    await asyncio.gather(
        user_client.run_until_disconnected(),
        bot_client.run_until_disconnected()
    )


if __name__ == "__main__":
    asyncio.run(main())
