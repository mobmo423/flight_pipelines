import smtplib
import os
from email.message import EmailMessage

class Alert():

    def has_failed(target_engine:object,log_table)->bool:
        '''
        this function will check from the log database if the function has failed
        '''
        sql_query = f'''
                    SELECT 
                        run_status  
                    FROM {log_table} 
                    ORDER BY run_timestamp desc
                    LIMIT 1 
                '''
        result = target_engine.execute(sql_query).fetchone()[0]
        if result !='completed': # test has failed
            return True
        return False 

    @staticmethod
    def connect_send(target_engine:object,log:str)->None:
        '''
        this function will send an email with the logs
        '''
        msg = EmailMessage()
        EMAIL_ADDRESS = os.environ.get('EMAIL_ADDRESS')
        EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD')

        msg['Subject'] = 'your pipeline has failed' 
        msg['From'] = EMAIL_ADDRESS
        # send an email to myself
        msg['To'] = EMAIL_ADDRESS
        msg.set_content(log)
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_ADDRESS,EMAIL_PASSWORD)
            smtp.send_message(msg)


if __name__ == '__main__':
    Alert.connect_send()