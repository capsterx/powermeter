#### END HEADER
def getresetcounter(byte):
    '''extract the reset counter from a byte'''
    return byte & 0b00000111      # same as 0x07

def getserial(packet):
    '''extract the ECM serial number from a compiled packet'''
    return "%d%05d" % (packet['unit_id'], packet['ser_no'])

def getserialraw(packet):
    '''extract the ECM serial number from a raw packet'''
    sn1 = ord(packet[26:27])
    sn2 = ord(packet[27:28]) * 256
    id1 = ord(packet[29:30])
    return "%d%05d" % (id1, sn1+sn2)

def obfuscate_serial(sn):
    '''obfuscate a brultech serial number - expose the last 3 digits of 6'''
    n = len(sn)
    return 'XXX%s' % sn[n-3:n]

def compare_packet_times(a, b):
    return cmp(a['time_created'], b['time_created'])

