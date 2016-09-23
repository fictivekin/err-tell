import logging
import os
import sqlite3
from errbot import botcmd, BotPlugin
from errbot.backends.base import RoomDoesNotExistError
from ago import human
from datetime import datetime
import threading

# This is ugly, but because the sqlite3 module is not thread-safe, this needs to be done.
# Ref: https://docs.python.org/3.5/library/sqlite3.html#multithreading
sqlite3_lock = threading.Lock()


class Tell(BotPlugin):
    """Saves a message to tell a user the next time they are seen in a channel"""

    unsent_counts = {}
    con = None
    cur = None

    def activate(self):
        TELLS_DB = self.plugin_dir + os.sep + 'tell.sqlite'
        if not os.path.exists(TELLS_DB):
            logging.warning('no database found, creating a new one')
            open(TELLS_DB, 'a').close()

        self.con = sqlite3.connect(TELLS_DB, check_same_thread=False)
        self.con.row_factory = sqlite3.Row # Allows for named results
        self.cur = self.con.cursor()
        try:
            logging.debug('Obtaining thread lock...')
            sqlite3_lock.acquire(True)
            self.cur.execute(TellSql.SQL_CREATE_TABLE_TELLS)
            self.con.commit()

        finally:
             sqlite3_lock.release()

        self.update_counts()

        super(Tell, self).activate()
        logging.debug('Tells activated.')


    def deactivate(self):
        self.con.close()
        super(Tell, self).deactivate()
        logging.debug('Tells deactivated.')


    def update_counts(self):
        self._update_unsent_counts()
        self._update_author_counts()


    def _update_unsent_counts(self):
        logging.debug('Updating unsent counts')
        try:
            logging.debug('Obtaining thread lock...')
            sqlite3_lock.acquire(True)
            self.cur.execute(TellSql.SQL_LOAD_UNSENT_COUNTS)
            msgs = self.cur.fetchall()
        finally:
            sqlite3_lock.release()

        self.unsent_counts = {}
        for msg in msgs:
            logging.debug('{}: {}'.format(msg['recipient'], msg['count']))
            self.unsent_counts.update({msg['recipient']: msg['count']})


    def _update_author_counts(self):
        logging.debug('Updating author counts')
        try:
            logging.debug('Obtaining thread lock...')
            sqlite3_lock.acquire(True)
            self.cur.execute(TellSql.SQL_LOAD_AUTHOR_COUNTS)
            all_msgs = self.cur.fetchall()
        finally:
            sqlite3_lock.release()

        self.author_counts = {}
        for msg in all_msgs:
            logging.debug('{}: {}'.format(msg['sender'], msg['count']))
            self.author_counts.update({msg['sender']: msg['count']})


    @botcmd(admin_only=True)
    def tellupdate(self, msg, args):
        """
           Updates the in memory counters of messages
        """

        self.update_counts()
        return 'Updated'


    @botcmd()
    def tellstatus(self, msg, args):
        """
           Shows counts of all the unsent tells by user
        """
        sender = str(msg.frm.nick)
        channel = str(msg.to)
        if channel == self.bot_identifier.nick:
            channel = sender

        logging.debug('Retrieving global status of tells')
        self.update_counts()

        if len(self.unsent_counts.keys()) == 0:
            return 'There are no tells waiting for anyone, {}'.format(sender)

        destination = self.build_identifier(channel)
        for recipient in self.unsent_counts.keys():
            if self.unsent_counts[recipient] > 1:
                self.send(destination, '{} messages are waiting for {}'.format(self.unsent_counts[recipient], recipient))
            else:
                self.send(destination, '{} message is waiting for {}'.format(self.unsent_counts[recipient], recipient))

        return 'That is all of the waiting tells, {}'.format(sender)


    @botcmd()
    def telllist(self, msg, args):
        """
           Lists all of the requesting user's unsent tells
           Usage:
               !tell list
        """

        sender = str(msg.frm.nick)
        channel = str(msg.to)
        if channel == self.bot_identifier.nick:
            channel = sender

        logging.info('Retrieving list of unsent tells saved by {}'.format(sender))

        try:
            logging.debug('Obtaining thread lock...')
            sqlite3_lock.acquire(True)
            self.cur.execute(TellSql.SQL_LIST_TELLS, (sender,))
            tells = self.cur.fetchall()
        finally:
            sqlite3_lock.release()

        destination = self.build_identifier(channel)

        is_at_least_one = False

        for tell in tells:
            if not is_at_least_one:
                self.send(destination,
                          'ID   | Recipient   | Channel  | Date      | Message')
                is_at_least_one = True

            self.send(destination,
                '{}  | {}  | {}  | {}  | {}'.format(
                    tell['id'],
                    tell['recipient'],
                    tell['channel'],
                    human(datetime.fromtimestamp(tell['created_ts']), 1),
                    tell['message']))

        if not is_at_least_one:
            if sender in self.author_counts and self.author_counts[sender] > 0:
                return 'None of your tells are unsent, {}'.format(sender)
            else:
                return 'You have not left a tell yet, {}'.format(sender)

        return 'That is all of your waiting tells, {}'.format(sender)


    @botcmd()
    def tellrm(self, msg, args):
        """
           Removes an unsent tell from the db if that tell was sent by you
           Usage:
               !tell rm 312
        """

        if len(args) != 1:
            return 'Usage: !tell rm <id>'

        sender = str(msg.frm.nick)
        tell_id = args[0]

        logging.debug('Removing tell {} for user {}'.format(tell_id, sender))

        try:
            logging.debug('Obtaining thread lock...')
            sqlite3_lock.acquire(True)
            self.cur.execute(TellSql.SQL_REMOVE_TELL, (sender, tell_id,))
            self.con.commit()
        finally:
            sqlite3_lock.release()

        return 'Removed: {}.'.format(tell_id)


    @botcmd()
    def tell(self, msg, args):
        """
           Adds a new message to be sent to a user the next time they are seen
           Usage:
               !tell someuser This is a message
        """

        sender = str(msg.frm.nick)
        channel = str(msg.to) # weird - but it makes sense if you think about it for a while
        if channel == self.bot_identifier.nick:
            return 'Tells can only be left in public channels.'

        logging.debug('Args are: {}'.format(args))
        recipient = args.split(' ')[0].replace('[,:;]$', '')
        message = ' '.join(args.split(' ')[1:])

        if len(message) == 0:
            return 'Tell has no message. I do apologize, but I\'m going to ignore it, {}.'.format(sender)

        if recipient == self.bot_identifier.nick:
            return 'Thanks for wanting to leave me a tell, {}, but why not just tell me now?'.format(sender)

        logging.debug('Saving tell from {} for {} in {}'.format(sender, recipient, channel))

        if not sender in self.author_counts:
            self.author_counts[sender] = 0

        self.author_counts[sender] += 1

        if not recipient in self.unsent_counts:
            self.unsent_counts[recipient] = 0

        self.unsent_counts[recipient] += 1

        try:
            logging.debug('Obtaining thread lock...')
            sqlite3_lock.acquire(True)
            self.cur.execute(TellSql.SQL_INSERT_TELL, (sender, channel, recipient, message,))
            self.con.commit()
        finally:
            sqlite3_lock.release()

        return 'Ok, {}. Message stored.'.format(sender)


    def send_tells(self, recipient):
        """
           Sends all waiting tells for a user and marks them as sent
        """

        logging.debug('Sending tells for {}'.format(recipient))

        try:
            logging.debug('Obtaining thread lock...')
            sqlite3_lock.acquire(True)
            self.cur.execute(TellSql.SQL_GET_TELLS_COUNTS_FOR_USER, (recipient,))
            counts = self.cur.fetchall()
        finally:
            sqlite3_lock.release()

        for room_count in counts:
            channel = room_count['channel']
            msg_count = room_count['count']
            if msg_count == 0:
                continue

            if self.is_user_in_channel(recipient, channel):
                try:
                    logging.debug('Obtaining thread lock...')
                    sqlite3_lock.acquire(True)
                    self.cur.execute(TellSql.SQL_GET_TELLS_FOR_USER_ROOM, (recipient, channel,))
                    tells = self.cur.fetchall()
                finally:
                    sqlite3_lock.release()

                channel_id = self.build_identifier(channel)
                for tell in tells:
                    self.send(channel_id,
                              self.prepare_message(
                                  recipient,
                                  tell['sender'],
                                  tell['created_ts'],
                                  tell['message']))
                    self.mark_as_sent(tell['id'])
                    self.unsent_counts[recipient] -= 1

            else:
                self.send_join_message(recipient, channel)

        # just in case something went wrong with the counts
        if self.unsent_counts[recipient] < 0:
            self.update_counts()



    def is_user_in_channel(self, recipient, channel):
        """
           Checks to see if a user is in a specific channel
        """

        for room in self.rooms():
            logging.debug('Checking if {} is {}'.format(channel, room))
            if room.room == channel:
                logging.debug('Room has these people in it: {}'.format(room.occupants))
                for occupant in room.occupants:
                    if recipient in occupant.nick:
                        return True

        return False


    def send_join_message(self, recipient, channel):
        """
           Invites a user to join a channel so that a tells can be delivered
        """
        self.send(self.build_identifier(recipient),
                  'Please join {}. I have unsent tells awaiting your presence.'.format(channel))


    def prepare_message(self, recipient, sender, time, message):
        """
           Formats a tell for sending to a user
        """
        # human returns the string suffixed with " ago", don't add it to the message manually.
        friendly_time = human(datetime.fromtimestamp(time), 1)
        return '{}: (from: {}, {}) {}'.format(recipient, sender, friendly_time, message)


    def mark_as_sent(self, tell_id):
        """
           Marks a message as sent in the db
        """
        try:
            logging.debug('Obtaining thread lock...')
            sqlite3_lock.acquire(True)
            self.cur.execute(TellSql.SQL_MARK_TELL_SENT, (tell_id,))
            self.con.commit()
        finally:
            sqlite3_lock.release()


    def callback_message(self, msg):
        """
           Checks all received IRC messages to see if tells need to be sent
        """

        author = str(msg.frm.nick)

        if author == self.bot_identifier.nick:
            # Ignore all messages the bot sends
            return

        if author in self.unsent_counts and self.unsent_counts[author] > 0:
            self.send_tells(author)



class TellSql():
    SQL_CREATE_TABLE_TELLS = '''
create table if not exists tells (
    id integer primary key,
    sender varchar(50) not null,
    channel varchar(50) not null,
    recipient varchar(50) not null,
    message text not null,
    is_sent tinyint(1) not null default 0,
    created_ts integer not null,
    sent_ts integer
)
'''

    SQL_INSERT_TELL = '''
insert into tells 
      (sender, channel, recipient, message, created_ts)
values(?, ?, ?, ?, strftime('%s', 'now'))
'''

    SQL_LIST_TELLS = '''
select id, recipient, channel, message,
       created_ts
  from tells
 where sender = ?
   and is_sent = 0
 order by created_ts desc
'''

    SQL_LOAD_UNSENT_COUNTS = '''
select count(*) as count,
       recipient
  from tells
 where is_sent = 0
 group by recipient
 order by recipient
'''

    SQL_LOAD_AUTHOR_COUNTS = '''
select count(*) as count,
       sender
  from tells
 group by sender
 order by sender
'''

    SQL_GET_TELLS_COUNTS_FOR_USER ='''
select count(*) as count,
       channel
  from tells
 where recipient = ?
   and is_sent = 0
 group by channel
'''

    SQL_GET_TELLS_FOR_USER_ROOM = '''
select id, sender, message,
       created_ts
  from tells
 where recipient = ?
   and channel = ?
   and is_sent = 0
 order by created_ts asc
'''

    SQL_MARK_TELL_SENT = '''
update tells
   set is_sent = 1,
       sent_ts = strftime('%s', 'now')
 where id = ?
'''

    SQL_REMOVE_TELL = '''
delete from tells
 where sender = ?
   and id = ?
'''

