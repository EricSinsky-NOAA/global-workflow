import os

COMIN_WAVE_RESTART_PREV = os.getenv('COMIN_WAVE_RESTART_PREV')
PDY = os.getenv('PDY')

file = COMIN_WAVE_RESTART_PREV + '/' + PDY  + '.030000.restart.glo_025'

with open(file, 'rb') as f:
    dat = bytearray(f.read())

i = 26
current_date_bytes = dat[i:i+10]
print("Current date bytes:", current_date_bytes)
dat[i:i+10] = b'2024-04-26'

with open(file, 'wb') as f:
    f.write(dat)
