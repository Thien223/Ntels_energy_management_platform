from cryptography.fernet import Fernet
import argparse

def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--renew_key', type=bool, default=False, help='Renew encrypt key')
    args = parser.parse_args()
    return args

def generate_encrypt_key():
    key = Fernet.generate_key()
    with open('key.bin', 'wb') as key_file: key_file.write(key)
    return key


def reset_login_info(renew_key=False):
    import getpass
    if renew_key == False:  ##GET KEY FROM FILE
        key = None
        with open('key.bin', 'rb') as key_file:
            for line in key_file:
                key = line
    else:  ### GENERATE NEW KEY ALSO SAVE TO FILE
        key = generate_encrypt_key()

    encoder = Fernet(key)
    db_username = input('Enter new databse username: ')
    db_password = getpass.getpass('Enter new databse password: ')
    ##confirm password
    i=0
    while i < 3:
        db_password_confirm = getpass.getpass('Confirm new databse password: ')
        i+=1
        if db_password==db_password_confirm:
            break
        else:
            if i==3:
                print('Retried 3 times, nothing changed.. Exiting')
                return
            else:
                print('Confirm password does not match.. retry..')


    ntels_username=input('Enter new ntels username: ')
    ntels_password=getpass.getpass('Enter new ntels password: ')
    ### confirm password
    j = 0
    while j < 3:
        ntels_password_confirm = getpass.getpass('Confirm new ntels password: ')
        j += 1
        if ntels_password == ntels_password_confirm:
            break
        else:
            if j==3:
                print('Retried 3 times, nothing changed.. Exiting')
                return
            else:
                print('Confirm password does not match.. retry..')







    encrypted_db_usename = encoder.encrypt(db_username.encode('utf-8'))
    encrypted_db_password = encoder.encrypt(db_password.encode('utf-8'))
    encrypted_ntels_username = encoder.encrypt(ntels_username.encode('utf-8'))
    encrypted_ntels_password = encoder.encrypt(ntels_password.encode('utf-8'))
    ### write data to info.bin1111111111111111111111111111111111111111111
    with open('info.dat', 'wb') as info_file:
        info_file.write(encrypted_db_usename + b'\n')
        info_file.write(encrypted_db_password + b'\n')
        info_file.write(encrypted_ntels_username + b'\n')
        info_file.write(encrypted_ntels_password + b'\n')
    print('New passwords changed successfully..')


if __name__ == '__main__':
    args=get_arguments()
    reset_login_info(renew_key=args.renew_key)
