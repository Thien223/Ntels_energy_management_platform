import tensorflow as tf
from cryptography.fernet import Fernet

def get_login_info():
    info=[]
    with open('info.dat', 'rb') as info_file:
        for line in info_file:
            info.append(line)
    encrypted_db_u = info[0]
    encrypted_db_p = info[1]
    encrypted_ntels_u = info[2]
    encrypted_ntels_p = info[3]
    return encrypted_db_u,encrypted_db_p,encrypted_ntels_u,encrypted_ntels_p


key=None
### get encrypt key from file
with open('key.bin', 'rb') as key_file:
    for line in key_file:
        key=line
encoder = Fernet(key)
encrypted_db_u,encrypted_db_p,encrypted_ntels_u,encrypted_ntels_p = get_login_info()

hparams = tf.contrib.training.HParams(
## Parameters
    db_host = '210.219.151.173',
    # db_host = '192.168.13.20',
    db_username = encoder.decrypt(encrypted_db_u).decode('utf-8'),
    db_password = encoder.decrypt(encrypted_db_p).decode('utf-8'),
    db_name = 'nisbcp',
    db_port = 3306,
    # ntels_host = 'https://192.168.10.6:18080',
    ntels_host = 'https://210.219.151.169:18080',
    # ntels_host = 'http://164.125.141.208:8080',
    login_url = '/NISBCP/login/doLogin.do',
    ntels_username = encoder.decrypt(encrypted_ntels_u).decode('utf-8'),
    ntels_password = encoder.decrypt(encrypted_ntels_p).decode('utf-8'),
    start_time="startDate=",
    end_time = "endDate=",
)
