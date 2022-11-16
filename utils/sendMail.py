import smtplib
from email.message import EmailMessage

def sendMail(asunto, mensaje):
	'''Envia un mail a partir de un asunto y mensaje a dos casillas de mail'''
	msg = EmailMessage()
	msg.set_content(mensaje)

	msg['Subject'] = asunto
	msg['From'] = 'python@radiomitre.com.ar'
	msg['To'] = 'pmpgbarrientos@radiomitre.com.ar,ainsua@radiomitre.com.ar'

	s = smtplib.SMTP('BUEXCG21.RMSA.COM')
	s.send_message(msg)
	s.quit()