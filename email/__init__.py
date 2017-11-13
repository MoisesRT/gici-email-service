import jinja2
import csv
import logging
from Queue import Queue
from os import path
from ast import literal_eval

groups = {}
users = {}
q = Queue()

logger = logging.getLogger('EMAIL_LOGGER')


class Group(object):
    def __init__(self, id):
        self.id = int(id)
        self.members = []

    def add_member(self, user):
        self.members.append(user)


class User(object):
    def __init__(self, id, first_name, last_name, email, company):
        self.id = int(id)
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.company = company
        self.email_preferences = {}

    def set_email_preferences(self, email_type, send_policy=True):
        if send_policy is not True:
            send_policy = int(send_policy)
        self.email_preferences[email_type] = bool(send_policy)


class Email(object):
    TEMPLATE_DIR = 'templates'
    SENT_EMAIL_DIR = 'sent_emails'

    def __init__(self, id, email_type, recipient_type, recipient_id, sender_email_address, subject_template,
                 body_template, data):
        self.id = int(id)
        self.email_type = email_type
        self.recipient_type = recipient_type
        self.recipient_id = int(recipient_id)
        self.sender_email_address = sender_email_address
        self.subject_template = subject_template
        self.body_template = body_template
        self.data = literal_eval(data)

    def send_mail(self):
        if self.recipient_type == 'direct':
            self.send_direct_mail(self.recipient_id)
        elif self.recipient_type == 'group_mail':
            for user_id in groups[self.recipient_id].members:
                self.send_direct_mail(user_id)

    def send_direct_mail(self, mail_recipient_id):
        if (self.email_type in users[mail_recipient_id].email_preferences) \
                and (users[mail_recipient_id].email_preferences[self.email_type] is True):
            logger.info('The mail with id {} will be sent to user {}'.format(self.id, mail_recipient_id))
            mail_subject_file = '{}_{}_subject.txt'.format(self.id, mail_recipient_id)
            mail_body_file = '{}_{}_body.html'.format(self.id, mail_recipient_id)
            env = jinja2.Environment(loader=jinja2.FileSystemLoader(self.TEMPLATE_DIR))
            subject_template = env.get_template(self.subject_template).render(self.data)
            body_template = env.get_template(self.body_template).render(self.data)
            file(path.join(self.SENT_EMAIL_DIR, mail_subject_file), 'wb').write(subject_template)
            file(path.join(self.SENT_EMAIL_DIR, mail_body_file), 'wb').write(body_template)


def send_mails():
    while q.qsize() > 0:
        cur_mail = q.get_nowait()
        cur_mail.send_mail()


def csv_reader(csv_type, input_file=None):
    INPUT_DIR = 'inputs'

    input_file = path.join(INPUT_DIR, csv_type + '.csv') if input_file is None else input_file
    with open(input_file, 'rb') as csv_file:
        reader = csv.reader(csv_file, delimiter='|')
        reader.next()
        for line in reader:
            if csv_type == 'group_members':
                group_id = int(line[0])
                if int(line[0]) not in groups:
                    groups[group_id] = Group(group_id)
                groups[group_id].add_member(int(line[1]))
            elif csv_type == 'users':
                users[int(line[0])] = User(*line)
            elif csv_type == 'user_email_preferences':
                users[int(line[0])].set_email_preferences(*line[1:])
            elif csv_type == 'emails':
                q.put(Email(*line))


def init():
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler('log.txt')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    csv_reader('group_members')
    csv_reader('users')
    csv_reader('user_email_preferences')
    csv_reader('emails')
