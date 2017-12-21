from create_video import create_video



# input_path_s = 'input_video/Easy/Chiaho_cut.mp4'
# input_path_t = 'input_video/Easy/Chiayi_cut.mp4'

# input_path_s = 'input_video/Easy/Yuho.mp4'
# input_path_t = 'input_video/Easy/MrRobot.mp4'


input_path_s = 'input_video/Medium/LucianoRosso1.mp4'
input_path_t = 'input_video/Medium/LucianoRosso3.mp4'

# input_path_s = 'input_video/Easy/Yuho.mp4' 
# input_path_t = 'input_video/Hard/Joker.mp4' 


len_frame = 150
output_path_s = 'output_video/output1.avi' 
output_path_t = 'output_video/output2.avi' 


create_video(input_path_s,input_path_t,len_frame,output_path_s,output_path_t)


