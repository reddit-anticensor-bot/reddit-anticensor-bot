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
        collation='utf8mb4_unicode_ci',
        use_unicode=True)

    c = mydb.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS topics (" \
        "id VARCHAR(12) NOT NULL, " \
        "copy_id VARCHAR(12), " \
        "author VARCHAR(25) NOT NULL, " \
        "subreddit VARCHAR(25) NOT NULL, " \
        "title VARCHAR(1024) NOT NULL, " \
        "text MEDIUMTEXT NOT NULL, " \
        "marked BOOL NOT NULL DEFAULT FALSE, " \
        "time BIGINT NOT NULL) " \
        "CHARACTER SET utf8mb4 " \
        "COLLATE utf8mb4_unicode_ci")

    c.execute("CREATE TABLE IF NOT EXISTS comments (" \
        "id VARCHAR(12) PRIMARY KEY, " \
        "parent_id VARCHAR(12) NOT NULL, " \
        "copy_id VARCHAR(12), " \
        "topic_id VARCHAR(12) NOT NULL, " \
        "author VARCHAR(25) NOT NULL, " \
        "text MEDIUMTEXT NOT NULL, " \
        "marked BOOL NOT NULL DEFAULT FALSE, " \
        "time BIGINT NOT NULL) " \
        "CHARACTER SET utf8mb4 " \
        "COLLATE utf8mb4_unicode_ci")

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
                  (topic.id, username, topic.subreddit.display_name, topic.title, topic.selftext, topic.created_utc))
        mydb.commit()

def get_user_prefs(username):
    c = mydb.cursor()
    c.execute("SELECT opted_out FROM users WHERE username = %s",(username,))
    rows = c.fetchall()
    if len(rows) != 0:
        return False, rows[0][0]
    else:
        return True, False

def remember_user(username):
    c = mydb.cursor()
    c.execute("INSERT INTO users (username) VALUES(%s)", (username,))
    mydb.commit()

def greeting_text(username):
    return "\n\n---\n\n/u/"+username+" your post has been copied because one or more comments in this topic " \
            "have been removed. This copy will preserve unmoderated topic. If you would " \
            "like to opt-out, please PM me."

def copy_topic(topic_id):
    c = mydb.cursor()
    c.execute("SELECT author, subreddit, title, text FROM topics WHERE id = %s", (topic_id,))
    author, subreddit, title, text = c.fetchone()
    
    title = "[ " + subreddit + " ] " + title
    title = title[:300]

    greet, opted_out = get_user_prefs(author)

    link = " [[link]](https://np.reddit.com/r/" + subreddit + "/comments/" + topic_id + ")"
    text = "Topic originally posted in " + subreddit + " by " + author + link + "\n\n---\n\n" + text

    if greet:
        text += greeting_text(author)
        remember_user(author)

    if opted_out:
        text = author + " has opted out out from this service."
    
    text += "\n\n---\n\n"

    sub = reddit.subreddit('u_' + config['reddit_username'])
    return sub.submit(title=title, selftext=text).id

def copy_comment(cmt_id, parent_cid, copy_tid):
    c = mydb.cursor()
    c.execute("SELECT author, text, topic_id FROM comments WHERE id = %s", (cmt_id,))
    author, text, topic_id = c.fetchone()
    c.execute("SELECT subreddit FROM topics WHERE id = %s", (topic_id,))
    subreddit = c.fetchone()[0]

    greet, opted_out = get_user_prefs(author)

    link = " [[link]](https://np.reddit.com/r/" + subreddit + "/comments/" + topic_id + "/_/" + cmt_id + ")"
    text = "Comment originally posted by " + author + link + "\n\n---\n\n" + text

    if greet:
        text += greeting_text(author)
        remember_user(author)

    if opted_out:
        text = author + " has opted out out from this service."

    if parent_cid == copy_tid:
        topic = reddit.submission(id=parent_cid)
        cpy = topic.reply(text)
    else:
        cmt = reddit.comment(id=parent_cid)
        cpy = cmt.reply(text)

    return cpy.id

def get_copy_topic_id(topic_id, is_deleted):
    c = mydb.cursor()
    c.execute("SELECT copy_id, marked FROM topics WHERE id=%s",(topic_id,))
    copy_id, marked = c.fetchone()
    if copy_id is None:
        copy_id = copy_topic(topic_id)
        c.execute("UPDATE topics SET copy_id = %s WHERE id = %s", (copy_id, topic_id))
        mydb.commit()
    
    if is_deleted and not marked:
        topic = reddit.submission(id=copy_id)
        text = "[ \U0001F534 DELETED \U0001F534 ] " + topic.selftext
        topic.edit(text)
        c = mydb.cursor()
        c.execute("UPDATE topics SET marked = 1 WHERE id = %s", (topic_id,))
        mydb.commit()

    return copy_id

def cmt_find_children(id):
    c = mydb.cursor()
    c.execute("SELECT id FROM comments WHERE parent_id=%s",(id,))
    rows = c.fetchall()
    return set([row[0] for row in rows])


def get_copy_comment_id(cmt_id, parent_cid, copy_tid, is_deleted):
    c = mydb.cursor()
    c.execute("SELECT copy_id, marked FROM comments WHERE id=%s",(cmt_id,))
    copy_id, marked = c.fetchone()
    if copy_id is None:
        copy_id = copy_comment(cmt_id, parent_cid, copy_tid)
        c.execute("UPDATE comments SET copy_id = %s WHERE id = %s", (copy_id, cmt_id))
        mydb.commit()

    if is_deleted and not marked:
        cmt = reddit.comment(id=copy_id)
        text = "[ \U0001F534 DELETED \U0001F534 ] " + cmt.body
        cmt.edit(text)
        cmt.mod.distinguish(how="yes", sticky=False)
        
        link = "\n\n[[deleted comment]](https://www.reddit.com/user/anticensor_bot/comments/" + copy_tid + "/_/" + copy_id + ")"
        topic = reddit.submission(id=copy_tid)
        text = topic.selftext + link
        topic.edit(text)

        c = mydb.cursor()
        c.execute("UPDATE comments SET marked = 1 WHERE id = %s", (cmt_id,))
        mydb.commit()        

    return copy_id

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

    deleted_set = set_offline - set_online
    if len(deleted_set) > 0:
        print("Hit on topic ", topic_id)
        copy_tid = get_copy_topic_id(topic_id, topic_id in deleted_set)
        copy_q = [(topic_id, copy_tid)]
        while copy_q:
            parent_id, parent_cid = copy_q.pop(0)
            for id in cmt_find_children(parent_id):
                cid = get_copy_comment_id(id, parent_cid, copy_tid, id in deleted_set)
                copy_q.append((id, cid))

if __name__ == "__main__":
    while True:
        ts = time.time()
        for sub in config['subreddits']:
            print("Discovering in sub ", sub)
            discover_topics(sub)
        for topic_id in get_topics_to_scan():
            print("Scanning in topic ", topic_id)
            scan_topic(topic_id)
        ts = 10*60 - (time.time() - ts)
        print("Sleeping for ", ts)
        if ts > 0:
            time.sleep(ts)
