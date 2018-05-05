CONFIG = {
    'crop_ss_dir':'./bbox_ss/',
    'crop_ct_dir':'./bbox/',
    'video_start' : 0,
    'video_end' : 100000,

    'width_upper': 130, # in pixel
    'width_lower': 40, # in pixel

    'heigth_upper' : 130, # 
    'heigth_lower' : 40,

    'area_upper' : 22500,
    'area_lower' : 3000,

    'expansion' : 0.15,
    'overlap' : 0.4,

    'surround' : 31,
    'deduct' :  2,

    'test_percent' : 0.1

}

CONFIG_BBOX = {
    'folder_name' : 'cell_xmls'
}


'''
config for different data
3895 :
    - upper lower 150 70
    - adapative threshold 41 2 

3891 :
    - upper lower 150 55
    - adapative threshold 41 2 

3892 :
    - upper lower 130 40
    - adapative threshold 41 2 

3893 :
    - upper lower 130 40
    - adapative threshold 31 2 



'''