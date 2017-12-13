# Tensorflow playground

This directory contains Tensorflow programs which can be used for testing and to
generate execution logs.

## Development Setup
Carefully read through the installation guides:
  * https://www.tensorflow.org/install/install_linux
  * http://www.nvidia.com/object/gpu-accelerated-applications-tensorflow-installation.html

Install the TF framework by running these commands:
```bash
$ conda create -n tensorflow
$ source activate tensorflow
$ pip install --ignore-installed --upgrade "https://storage.googleapis.com/tensorflow/linux/gpu/tensorflow_gpu-1.0.1-cp27-none-linux_x86_64.whl
  # (or if CPU only: "https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow-1.0.1-cp27-none-linux_x86_64.whl")
$ pip install --ignore-installed --upgrade "https://storage.googleapis.com/tensorflow/linux/cpu/protobuf-3.1.0-cp27-none-linux_x86_64.whl"
```

Test that the GPU driver is installed correctly:
```bash
$ ls /dev/nvidia*
/dev/nvidia0  /dev/nvidia1  /dev/nvidiactl  /dev/nvidia-uvm  /dev/nvidia-uvm-tools
$ cat /proc/driver/nvidia/version
NVRM version: NVIDIA UNIX x86_64 Kernel Module  367.48  Sat Sep  3 18:21:08 PDT 2016
GCC version:  gcc version 5.4.0 20160609 (Ubuntu 5.4.0-6ubuntu1~16.04.4) 
```

You will need to set some environment variables either in `~/.profile` or using something like [direnv](https://direnv.net/):
```bash
# NVIDIA CUDA [http://docs.nvidia.com/cuda/cuda-installation-guide-linux/#axzz4VZnqTJ2A]
CUDA_HOME="/usr/local/cuda-8.0"
if [[ -d "${CUDA_HOME}" ]]; then
  export PATH="${CUDA_HOME}/bin:${PATH}"
  export LD_LIBRARY_PATH="${CUDA_HOME}/lib64:${LD_LIBRARY_PATH}"
  # CUPTI = CUDA Profiling Tools Interface -> needed for `libcupti.so`
  export LD_LIBRARY_PATH="${CUDA_HOME}/extras/CUPTI/lib64:${LD_LIBRARY_PATH}"
fi

# Google's Bazel build system (if you decide to build Tensorflow from source)
PATH_add "bazel/bin/"
source "bazel/lib/bazel/bin/bazel-complete.bash"
```

Run the benchmark programs:
* [Image classification with AlexNet](https://github.com/DS3Lab/ImageClassification/tree/master/tensorflow/AlexNet).
  The main program to run is `classifier.py` which will load some images and train
  the neural network.

Augment the `session.run(...)` calls with additional options; this will enable tracing which produces detailed activity timelines:
* Source: http://stackoverflow.com/questions/34293714/can-i-measure-the-execution-time-of-individual-operations-with-tensorflow
* Discussion: https://github.com/tensorflow/tensorflow/issues/1824
