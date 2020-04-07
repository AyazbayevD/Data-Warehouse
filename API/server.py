import socket

HOST = ''
PORT = ''


def recevive_files():
    s = socket.socket()
    s.bind((HOST, PORT))
    s.listen(5)
    print("Server listening...")
    cnt = 0
    while True:
        conn, addr = s.accept()
        print('Got connection from', addr)
        data = conn.recv(1024)
        print('data=%s', (data))
        if not data:
            break
        format = str(data).split('.')[1]
        filename = f'temp_files/file_{cnt}.{format}'
        file = open(filename, 'w')
        file.write(data)
