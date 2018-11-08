"""
This module represents a device.

Computer Systems Architecture Course
Assignment 1
March 2018
"""

from threading import Event, Thread, Lock, Condition, Semaphore
from barrier import ReusableBarrier
import multiprocessing

class Device(object):
    """
    Class that represents a device.
    """

    def __init__(self, device_id, sensor_data, supervisor):
        """
        Constructor.

        @type device_id: Integer
        @param device_id: the unique id of this node; between 0 and N-1

        @type sensor_data: List of (Integer, Float)
        @param sensor_data: a list containing (location, data) as measured by this device

        @type supervisor: Supervisor
        @param supervisor: the testing infrastructure's control and validation component
        """

        self.device_id = device_id
        self.sensor_data = sensor_data
        self.supervisor = supervisor
        self.script_received = Event()
        self.scripts = []
        self.devices = []
        self.neighbours = []
        self.mutex = Lock()
        self.device_semaphores = {}
        self.worker_threads = []
        self.workers_number = 8 # this is the maximum number of workers
        self.master_thread = None
        self.barrier = None
        self.timepoint_done = False
        self.script_in_progress = Lock()
        self.global_lock = None
        self.max_workers = 8
        self.worker_start_condition = []
        self.workers_finished = 0
        self.device_lock = Lock()
        self.exit_workers = 0
        self.all_workers_finished = Event()
        self.can_receive_scripts = Event()
        self.can_receive_scripts.set()
        self.all_workers_joined = Event()
        self.can_calculate_workers_number = Event()
        self.can_calculate_workers_number.set()

        for i in range(0, self.max_workers):
            self.worker_start_condition.append(Event())

    def __str__(self):
        """
        Pretty prints this device.

        @rtype: String
        @return: a string containing the id of this device
        """
        return "Device %d" % self.device_id

    def setup_devices(self, devices):
        """
        Setup the devices before simulation begins.

        @type devices: List of Device
        @param devices: list containing all devices
        """
        
        self.devices = devices
        
        barrier = ReusableBarrier(len(devices))
        lock = Lock()
        aux_dict = {}

        for device in devices:
            device.barrier = barrier
            device.global_lock = lock
            for location in device.sensor_data:        
                if location not in aux_dict:
                    aux_dict[location] = Semaphore()            
        
        for device in devices:
            device.device_semaphores = aux_dict

        self.setup_master_thread()

    def setup_master_thread(self):
        self.master_thread = DeviceMaster(self)
        self.master_thread.start()

    def setup_worker_threads(self):
        """ Setup the worker threads """
    
        for thread_number in range(0, self.max_workers):
            worker = DeviceWorker(self, thread_number)
            self.worker_threads.append(worker)
            worker.start()
 
    def assign_script(self, script, location):
        """
        Provide a script for the device to execute.

        @type script: Script
        @param script: the script to execute from now on at each timepoint; None if the
            current timepoint has ended

        @type location: Integer
        @param location: the location for which the script is interested in
        """
        self.can_receive_scripts.wait()

        if script is not None:
            self.scripts.append((script, location))
            self.script_received.set()
        else:
            self.script_received.set()        
            self.timepoint_done = True         
                            
        # Updating the dict of semaphores whenever we assign a new location

        semaphore = Semaphore()

        if location not in self.device_semaphores:
            for device in self.devices:
                device.device_semaphores[location] = semaphore

    def get_data(self, location):
        """
        Returns the pollution value this device has for the given location.

        @type location: Integer
        @param location: a location for which obtain the data

        @rtype: Float
        @return: the pollution value
        """
        
        if location in self.sensor_data:
            pollution_value = self.sensor_data[location]
        else:
            pollution_value = None

        return pollution_value

    def compute_workers_number(self):

        self.can_calculate_workers_number.wait()
        
        if len(self.scripts) < 8:
            self.workers_number = len(self.scripts)
        else:
            self.workers_number = 8
 
    def wake_up_workers(self):
        if self.workers_number == 0:
            self.all_workers_finished.set()
            self.workers_finished = 0
        else:
            for i in range(0, self.workers_number):
                self.worker_start_condition[i].set()
            
    def set_data(self, location, data):
        """
        Sets the pollution value stored by this device for the given location.

        @type location: Integer
        @param location: a location for which to set the data

        @type data: Float
        @param data: the pollution value
        """

        if location in self.sensor_data:
            self.sensor_data[location] = data

    def shutdown_master_thread(self):

        self.master_thread.join()

    def shutdown_worker_threads(self):

        for thread in self.worker_threads:
            thread.join()        

    def shutdown(self):
        """
        Instructs the device to shutdown (terminate all threads). This method
        is invoked by the x. This method must block until all the threads
        started by this device terminate.
        """
        self.all_workers_joined.wait()        
        self.shutdown_master_thread()
        self.all_workers_joined.clear()
        
class DeviceMaster(Thread):
    """
    Class that implements the device's worker thread.
    """    
    def __init__(self, device):
        """

        Constructor.
        @type device: Device
        @param device: the device which owns this thread
        """
        Thread.__init__(self, name="Device Thread %d" % device.device_id)
        self.device = device
  
    def run(self):

        self.device.setup_worker_threads()
        
        while True:

            self.device.neighbours = self.device.supervisor.get_neighbours()
       
            if self.device.neighbours is None:
                break                         
            
            self.device.barrier.wait()

            while True:
                
                self.device.script_received.wait()
                self.device.script_received.clear()
                
                self.device.compute_workers_number()
                self.device.can_calculate_workers_number.clear()

                if self.device.workers_number != 0:
                    self.device.can_receive_scripts.clear()
                    self.device.wake_up_workers()
                    self.device.all_workers_finished.wait()
                    self.device.all_workers_finished.clear()            
                    self.device.workers_finished = 0
                    
                self.device.can_calculate_workers_number.set()
                self.device.can_receive_scripts.set()
                if self.device.timepoint_done:
                    self.device.timepoint_done = False                    
                    self.device.barrier.wait()
                    break

        self.device.exit_workers = 1
        self.device.workers_number = self.device.max_workers
        self.device.wake_up_workers()
        self.device.shutdown_worker_threads()
        self.device.all_workers_joined.set()
    
class DeviceWorker(Thread):
    """
    Class that implements the device's worker thread.
    """    
    def __init__(self, device, thread_id):
        """
        Constructor.

        @type device: Device
        @param device: the device which owns this thread
        """
        Thread.__init__(self, name="Device Thread %d" % device.device_id)
        self.device = device
        self.thread_id = thread_id
 
    def run(self):
        
        while True:
            self.device.worker_start_condition[self.thread_id].wait()
            self.device.worker_start_condition[self.thread_id].clear()

            if self.device.exit_workers:
                break

            script_size = len(self.device.scripts)
            start_index = self.thread_id * script_size / self.device.workers_number
            end_index = (self.thread_id + 1) * script_size / self.device.workers_number

            if self.thread_id == (self.device.workers_number - 1):
                end_index = script_size

            for index in range(start_index, end_index):
    
                (script, location) = self.device.scripts[index]
                
                self.device.device_semaphores[location].acquire()
                
                script_data = []
                # collect data from current neighbours
                for device in self.device.neighbours:
                    data = device.get_data(location)
                    if data is not None:
                        script_data.append(data)

                # add our data, if any
                data = self.device.get_data(location)
                if data is not None:
                    script_data.append(data)

                if script_data != []:
                    # run script on data
                    result = script.run(script_data)

                    # update data of neighbours, hope no one is updating at the same time
                    for device in self.device.neighbours:
                        device.set_data(location, result)
                    # update our data, hope no one is updating at the same time
                    self.device.set_data(location, result)

                self.device.device_semaphores[location].release()
            
            with self.device.device_lock:
                self.device.workers_finished += 1
                if self.device.workers_finished == self.device.workers_number:
                    self.device.all_workers_finished.set()            
            
