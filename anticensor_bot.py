# This code is licensed under CC0.
# Feel free to do whatever you want with it
# https://creativecommons.org/publicdomain/zero/1.0/

import json
import mysql.connector
import praw
import time

def startup():
    with open('config.json', 'r', encoding='utf-8') as filedata:
        config = json.load(filedata)

    mydb = mysql.connector.connect(host=config['mysql_host'],
        user=config['mysql_user'],
        passwd=config['mysql_passwd'],
        database=config['mysql_database'],
        charset='utf8mb4',
        collation='utf8mb4_bin',
        use_unicode=True)

    c = mydb.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS topics (" \
        "id VARCHAR(12) NOT NULL, " \
        "copy_id VARCHAR(12), " \
        "author VARCHAR(25) NOT NULL, " \
        "subreddit VARCHAR(25) NOT NULL, " \
        "title VARCHAR(1024) NOT NULL, " \
        "text MEDIUMTEXT NOT NULL, " \
        "time BIGINT NOT NULL) " \
        "CHARACTER SET utf8mb4 " \
        "COLLATE utf8mb4_bin")

    c.execute("CREATE TABLE IF NOT EXISTS comments (" \
        "id VARCHAR(12) PRIMARY KEY, " \
        "parent_id VARCHAR(12) NOT NULL, " \
        "copy_id VARCHAR(12), " \
        "topic_id VARCHAR(12) NOT NULL, " \
        "author VARCHAR(25) NOT NULL, " \
        "text MEDIUMTEXT NOT NULL, " \
        "time BIGINT NOT NULL) " \
        "CHARACTER SET utf8mb4 " \
        "COLLATE utf8mb4_bin")

    c.execute("CREATE TABLE IF NOT EXISTS users (" \
        "username VARCHAR(25) NOT NULL, " \
        "opted_out BOOL DEFAULT FALSE)")

    reddit = praw.Reddit(client_id=config['reddit_client_id'], 
        client_secret=config['reddit_client_secret'],
        username=config['reddit_username'],
        password=config['reddit_password'],
        user_agent=config['reddit_user_agent'])
    return config, mydb, reddit


config, mydb, reddit = startup()

def topic_in_db(id):
    c = mydb.cursor()
    c.execute("SELECT * FROM topics WHERE id=%s",(id,))
    return len(c.fetchall()) != 0

def get_comments_for_topic(topic_id):
    c = mydb.cursor()
    c.execute("SELECT id FROM comments WHERE topic_id=%s",(topic_id,))
    rows = c.fetchall()
    return set([row[0] for row in rows])

def get_topics_to_scan():
    c = mydb.cursor()
    c.execute("SELECT id FROM topics WHERE unix_timestamp()-time < 7*24*60*60")
    rows = c.fetchall()
    return [row[0] for row in rows]
    
def discover_topics(subred):
    for topic in reddit.subreddit(subred).new(limit=100):
        if topic_in_db(topic.id):
            continue

        if topic.author is None:
            username = "[deleted]"
        else:
            username = topic.author.name

        c = mydb.cursor()
        c.execute("INSERT INTO topics (id, author, subreddit, title, text, time) VALUES (%s, %s, %s, %s, %s, %s)",
                  (topic.id, username, topic.subreddit.name, topic.title, topic.selftext, topic.created_utc))
        mydb.commit()

def get_user_prefs(username):
    c = mydb.cursor()
    c.execute("SELECT opted_out FROM users WHERE username = %s",(username,))
    rows = c.fetchall()
    if len(rows) != 0:
        return False, rows[0][0]
    else:
        return True, rows[0][0]

def remember_user(username):
    c = mydb.cursor()
    c.execute("INSERT INTO users (username) VALUES(%s)", (username,))
    mydb.commit()

def greeting_text(username):
    return "\n\n---/u/"+username+" your post has been copied because one or more comments " \
            "have been removed by a moderator. This copy will preserve unmoderated topic. If you would " \
            "like to opt-out, please PM me."

def copy_topic(topic_id):
    c = mydb.cursor()
    c.execute("SELECT author, subreddit, title, text FROM topics WHERE id = %s", (topic_id,))
    author, subreddit, title, text = c.fetchone()
    
    title = "[ " + subreddit + " ] " + title
    greet, opted_out = get_user_prefs(author)

    text = "Topic originally posted in " + subreddit + " by " + author + "\n---\n" + text
    if greet:
        text += greeting_text(author)
        remember_user(author)

    if opted_out:
        text = author + " has opted out out from this service."

    sub = reddit.subreddit('u_' + config['reddit_username'])
    return sub.submit(title=title, selftext=text)

def copy_comment(cmt_id, parent_cid, copy_tid):
    c = mydb.cursor()
    c.execute("SELECT author, text FROM comments WHERE id = %s", (cmt_id,))
    author, text = c.fetchone()
    greet, opted_out = get_user_prefs(author)

    text = "Comment originally posted by " + author + "\n---\n" + text

    if greet:
        text += greeting_text(author)
        remember_user(author)

    if opted_out:
        text = author + " has opted out out from this service."

    if parent_cid == copy_tid:
        topic = reddit.submission(id=parent_cid)
        id = topic.reply(text)
    else:
        cmt = reddit.comment(id=parent_cid)
        id = cmt.reply(text)

    return id

def get_copy_topic_id(topic_id):
    c = mydb.cursor()
    c.execute("SELECT copy_id FROM topics WHERE id=%s",(topic_id,))
    rows = c.fetchall()
    if rows[0][0] is None:
        id = copy_topic(topic_id)
        c.execute("UPDATE topics SET copy_id = %s WHERE id = %s", (id, topic_id))
        mydb.commit()
        return id
    else:
        return rows[0][0]

def cmt_find_children(id):
    c = mydb.cursor()
    c.execute("SELECT id FROM comments WHERE parent_id=%s",(id,))
    rows = c.fetchall()
    return set([row[0] for row in rows])

def get_copy_comment_id(cmt_id, parent_cid, copy_tid):
    c = mydb.cursor()
    c.execute("SELECT copy_id FROM comments WHERE id=%s",(cmt_id,))
    rows = c.fetchall()
    if rows[0][0] is None:
        id = copy_comment(cmt_id, parent_cid, copy_tid)
        c.execute("UPDATE comments SET copy_id = %s WHERE id = %s", (id, cmt_id))
        mydb.commit()
        return id
    else:
        return rows[0][0]

def scan_topic(topic_id):
    dct_online = dict()
    topic = reddit.submission(id=topic_id)
    
    if topic.removed_by_category is None:
        dct_online[topic_id] = topic

    topic.comments.replace_more(limit=None)
    for cmt in topic.comments.list():
        if cmt.author is None or cmt.body == '[removed]' or cmt.body == '[deleted]':
            continue
        dct_online[cmt.id] = cmt

    set_offline = get_comments_for_topic(topic_id)
    set_offline.add(topic_id) # If we are scanning it, it is stored
    set_online = set(dct_online.keys())

    set_store = set_online - set_offline

    if len(set_store) >  0:
        c = mydb.cursor()
        for cid in set_store:
            cmt = dct_online[cid]
            c.execute("INSERT INTO comments (id, parent_id, topic_id, author, text, time) VALUES (%s, %s, %s, %s, %s, %s)",
                      (cmt.id, cmt.parent_id[3:], topic_id, cmt.author.name, cmt.body, cmt.created_utc))
        mydb.commit()

    has_deleted = len(set_offline - set_online) > 0
    if has_deleted:
        copy_tid = get_copy_topic_id(topic_id)
        copy_q = [(topic_id, copy_tid)]
        while copy_q:
            parent_id, parent_cid = copy_q.pop(0)
            for id in cmt_find_children(parent_id):
                cid = get_copy_comment_id(id, parent_cid, copy_tid)
                copy_q.append((id, cid))

if __name__ == "__main__":
    while True:
        ts = time.time()
        for sub in config['subreddits']:
            discover_topics(sub)
        for topic_id in get_topics_to_scan():
            scan_topic(topic_id)
        ts = time.time() - ts
        time.sleep(10*60-ts)
