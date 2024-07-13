import os
from glob import glob
from setuptools import find_packages, setup

package_name = 'data_preparation'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        (os.path.join('share', package_name, 'launch'), glob(os.path.join('launch', '*launch.[pxy][yma]*'))),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='gugafelds',
    maintainer_email='gustavotb_barros@hotmail.com',
    description='TODO: Package description',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'rosbag_processor = data_preparation.rosbag_processor:main',
            'rosbag_gui = data_preparation.rosbag_gui:main',
            'connect_socket = data_preparation.connect_socket:main',
        ],
    },
)
