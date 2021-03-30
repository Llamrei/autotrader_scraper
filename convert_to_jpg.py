# coding: utf-8
from PIL import Image
import io
import pickle as pkl
data = pkl.load(open('backup_11_21.pickle','rb'))
im = data[0]['images'][0]
im_f = io.BytesIO(im)
image = Image.open(im_f).convert('RGB')
with open('test.jpg','wb') as f:
    image.save(f, "JPEG", quality=85)
    
