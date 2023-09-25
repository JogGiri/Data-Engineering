import os
import shutil

current_directory = os.getcwd()
destination_directory = current_directory + '/directories'
# print(current_directory)

# creating demo directories 
# using makedirs instead mkdir
os.makedirs(destination_directory+'/videos',exist_ok=True)
os.makedirs(destination_directory+'/music',exist_ok=True)
os.makedirs(destination_directory+'/images',exist_ok=True)
os.makedirs(destination_directory+'/documents',exist_ok=True)
os.makedirs(destination_directory+'/bin',exist_ok=True)

# print('all files:', os.listdir())
# print('destination directories: ',os.listdir(current_directory))

def organizer(fileName,fileType):

    audio = ("3ga", "aac", "ac3", "aif", "aiff","alac", "amr", "ape", "au", "dss","flac", "flv", "m4a", "m4b", "m4p","mp3", "mpga", "ogg", "oga", "mogg",
         "opus", "qcp", "tta", "voc", "wav","wma", "wv")

    video = ("webm", "MTS", "M2TS", "TS", "mov","mp4", "m4p", "m4v", "mxf",'mkv')

    img = ("jpg", "jpeg", "jfif", "pjpeg", "pjp", "png","gif", "webp", "svg", "apng", "avif",'bmp')

    docs = ('pdf','docs','ppt')

    


    if fileType in audio:
        shutil.move(current_directory+'/'+fileName,destination_directory+'/music')
        print(fileName,'is moved to music')

    elif fileType in video:
        shutil.move(current_directory+'/'+fileName,destination_directory+'/video')
        print(fileName,'is moved to video')

    elif fileType in img:
        shutil.move(current_directory+'/'+fileName,destination_directory+'/images')
        print(fileName,'is moved to images')

    elif fileType in docs:
        shutil.move(current_directory+'/'+fileName,destination_directory+'/documents')
        print(fileName,'is moved to documents')

    else:
        if fileType == 'py':
            print(fileName + 'is a python file')
        else:
            shutil.move(current_directory+'/'+fileName,destination_directory+'/bin')
            print(fileName,' is moved to bin')
        

for i in os.listdir():
    if '.' in i:
        z=i.split('.')
        print('fileName :',i ,'extension',z[-1])
        organizer(i,z[-1])


