#!/bin/bash
docker create -t -i --privileged --name chfs -v $(pwd):/home/stu/chfs kecelestial/chfs_image bash