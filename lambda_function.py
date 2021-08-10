import sys, os, re, base64, json, boto3
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'packages'))

class Globals:
	def __init__(self): pass
	
	def get_db(self):
		if not hasattr(self, 'mysql_db'): self.mysql_db = self.db_connect()
		return self.mysql_db
	
	def db_connect(self):
		import pymysql
		connect_args = {'cursorclass': pymysql.cursors.DictCursor}
		connect_args['host'] = os.environ.get('MYSQL_DATABASE_HOST')
		connect_args['port'] = os.environ.get('MYSQL_DATABASE_PORT', 3306)
		connect_args['user'] = os.environ.get('MYSQL_DATABASE_USER')
		connect_args['password'] = os.environ.get('MYSQL_DATABASE_PASSWORD')
		connect_args['db'] = os.environ.get('MYSQL_DATABASE_DB')
		connect_args['charset'] = os.environ.get('MYSQL_DATABASE_CHARSET', 'utf8mb4')
		return pymysql.connect(**connect_args)
	
	def db_close(self):
		if hasattr(self, 'mysql_db'):
			self.mysql_db.close()
			del self.mysql_db
	
	# ----------------------------------------
	
	def get_lmtp(self):
		if not hasattr(self, 'lmtp'): self.lmtp = self.lmtp_connect()
		return self.lmtp
	
	def lmtp_connect(self):
		import smtplib
		
		lmtp = smtplib.LMTP(os.environ.get('LMTP_HOST'),
			os.environ.get('LMTP_PORT', 2525),
			local_hostname=os.environ.get('LMTP_LOCAL_HOSTNAME'))
		return lmtp
	
	def lmtp_close(self):
		if hasattr(self, 'lmtp'):
			self.lmtp.quit()
			del self.lmtp

g = Globals()

def splitaddr(addr):
	m = re.search(r'([^<>@]+)@([^<>@]+)', addr)
	if m is None: return (None, None)
	return (v.lower() for v in m.groups())

def process_message(message):
	try:
		if message['mail']['messageId'] == 'AMAZON_SES_SETUP_NOTIFICATION':
			print('AMAZON_SES_SETUP_NOTIFICATION')
			return False
	except: pass
	
	
	# ---------- Return-Path
	try:
		returnPath = message['mail']['commonHeaders']['returnPath']
	except:
		returnPath = ''
	
	
	# ---------- Logging
	try:
		commonHeaders = message['mail']['commonHeaders']
		print('From', commonHeaders.get('from')) # ['Jane Doe <...>', ...]
		print('To', commonHeaders.get('to'))
		print('Subject', commonHeaders.get('subject'))
	except:
		pass
	
	
	# ---------- Body
	if 'content' in message:
		# SNS Action in Receiving Rule
		
		eml = base64.b64decode(message['content'])
	else:
		try:
			action = message['receipt']['action']
			obj = boto3.client('s3').get_object(Bucket=action['bucketName'], Key=action['objectKey'])
			eml = obj['Body'].read()
		except:
			print('Error while get mail from S3')
			return False
	
	
	# ---------- SPAM check
	'''
	receipt = message['receipt']
	for key in ('dkim', 'spam', 'spf', 'virus'):
		receipt[key+'Verdict']['status'] === 'PASS' / 'FAIL'
	'''
	
	
	# ----------  Recipients, Aliases and Send
	recipients = message['receipt']['recipients']
	for recipient in recipients:
		username, domain = splitaddr(recipient)
		if domain is None:
			print(f'Parse error: {recipient}')
			return False
		
		lmtp = g.get_lmtp()
		
		# alias lookup
		conn = g.get_db()
		cursor = conn.cursor()
		cursor.execute("SELECT `destination` FROM `aliases` WHERE `active` = 'Y' AND (`source` = %s OR `source` = %s);",
			(f'@{domain}', recipient.lower()))
		
		result = cursor.fetchall()
		for row in result:
			print('alias send', row['destination'])
			lmtp.sendmail(returnPath, row['destination'], eml)
		
		if len(result) == 0:
			print('send', recipient)
			lmtp.sendmail(returnPath, recipient, eml)
	
	return True

def lambda_handler(event, context):
	'''
	https://docs.aws.amazon.com/ses/latest/DeveloperGuide/receiving-email-action-lambda-event.html
	
	- Lambda Action in Receiving Rule
		cannot invoke a lambda function in other region.
		cannot read the mail body.
	
	- SNS Action in Receiving Rule
		only emails that are 150 KB or less. Larger emails will bounce.
		https://docs.aws.amazon.com/ses/latest/DeveloperGuide/receiving-email-action-sns.html?icmpid=docs_ses_console
	
	v S3 Action in Receiving Rule, add SNS topic when S3 action is performed
	  and Lambda function subscribes the topic.
	'''
	
	records = event.get('Records', [])
	for record in records:
		if 'EventSource' not in record or record['EventSource'] != 'aws:sns':
			print("EventSource != 'aws:sns'")
			continue
		
		try:
			message = json.loads(record['Sns']['Message'])
		except:
			print('Error while loads Sns.Message')
			continue
		
		if 'notificationType' in message and message['notificationType'] == 'Received':
			process_message(message)
		else:
			print("notificationType != 'Received'")
			continue
	
	g.db_close()
	g.lmtp_close()
	return 'done'
