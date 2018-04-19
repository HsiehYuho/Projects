# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 13:30:15 2018

@author: Owen
"""
from PIL import Image
import torch
import numpy as np
import matplotlib.pyplot as plt
from torch.autograd import Variable



# load numpy image 
img_np = np.load('image_sample.npy')

img = Image.fromarray(np.uint8(img_np))

img_np = np.array(img.resize((64,64)))


img_torch = torch.from_numpy(img_np).transpose(0,2).transpose(1,2).unsqueeze(0).float()

img_torch = Variable(img_torch.cuda())

model = torch.load('best_model.pt')

prediction = model(img_torch)

prediction_np = prediction.cpu().data.numpy()

# final predicted label
label = np.argmax(prediction_np)

print("predicted label: ", label)











