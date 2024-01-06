from setup import *

def delete_data(dwh_tables):
    for i in dwh_tables:
        path = "/datawarehouse/{}".format(i)
        status = fs.listStatus(Path(path))
        print(status)
        # for fileStatus in status:
        #     sub_file_path = fileStatus.getPath()
        #     print(sub_file_path)
            # if sub_file_path:
            #     fs.delete(sub_file_path)
            #     print("Deleted Data!")

            # else:
            #     print("No Data!")

delete_data(dwh_tables)


    
