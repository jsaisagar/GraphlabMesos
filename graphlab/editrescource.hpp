/* In this file we will go through the procedure of
 * accepting the resources offered to graphlab and submitting the task to the mesos
 * executor. Some of the values are assumed to be constant. For 
 * convenience, I used some of the templates with the name that could be recogized 
 * easily. Please contact jsaisagar@gmail.com for further queries.
 * */
#ifndef RESOURCE_CALCULATION
#define RESOURCE_CALCULATION
#include <libgen.h>

#include <iostream>
#include <string>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>

#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>
#include <stout/check.hpp>
#include <stout/flags.hpp>
#include <stout/os.hpp>
#include <stout/stringify.hpp>
#include </home/test/mesos-0.14.0/src/logging/flags.hpp>
#include <graphlab.hpp>
#include <graphlab/scheduler/ischeduler.hpp>
#include <graphlab/scheduler/scheduler_factory.hpp>
using mesos::Resource;
using mesos::Offer;
using mesos::OfferID;
using mesos::SchedulerDriver;
using mesos::TaskStatus;
using mesos::ExecutorID;
using mesos::ExecutorInfo;
using mesos::FrameworkInfo;
using mesos::SlaveID;
using mesos::TaskInfo;
using mesos::Value;
using mesos::Value_Scalar;
using std::string;
using std::vector;

using namespace graphlab;
typedef distributed_graph<float , graphlab::empty> graph_type;
const size_t cpus_graphlab = 6;
const size_t mem_graphlab = 5.441520;
const size_t cpus_task = 1;
const size_t mem_task = 1;
class resourceCalculation {
private:

    size_t size_of_queue;
    size_t vertices;

public:
    int *array_cpu;
    vector<Offer> offer;
    SchedulerDriver* driver;
     template <class warp_engine_edited>
     class dirty {
	public: 
		warp_engine_edited *point;
		void set_pointer(warp_engine_edited *ptr)
		{
			point = ptr;
		}
		warp_engine_edited* return_pointer()
		{
			return point;
		}
	};
void setOffer(const vector<Offer>& offered, SchedulerDriver* drivered)
        {
                 offer = offered;
		 driver = drivered;
		 //resource(driver,offer);
		 
        }
/*
 * This function takes the context of warp engine from the file "graph.hpp". Along
 * with context, it also takes the pointer to driver, vector of offers which mesos 
 * allocated to Graphlab and a constant reference to Executor Information.
 * The vector offer will be in the form of "<<2 CPUs, 1 GB RAM> , <4 CPUs, 2 GB RAM>>" where
 * each vector is from one machine of the mesos cluster.
 *
 * We would take offer from each machine and see whether it is acceptable for us or not against
 * the resources required for graphlab. It would be ideal to take the memory required of graphlab 
 * from "warp_engine_edited.hpp".
 * 
 * I also did the task which in my perspective is launching the fibers. From here I faced difficulties.
 * Since the executor should be the executable, the task of mesos should be chosen so that we can execute the program directly from mesos executor. 
 * In my work, I called the threading function directly from Mesos Scheduler. 
 * Identification of the best task would solve this difficulty. But ideal work would be to call the threading function from executor. 
 * In executor right now, I simply printed the task numbers.
 * 
 * The commented code in the function may also help in understanding
 * */
template <class warp_engine_edited>
void resource(SchedulerDriver* driver, const vector<Offer>& offer, warp_engine_edited *edited, const ExecutorInfo& executorInfo )
{
	edited->aggregator;
        string uri =  "home/test/GRAPHLABS/graphlab/src/graphlab/engine/executor";
        for(size_t i =0; i < offer.size(); i++) 
	{
        	const Offer& singleSystem = offer[i];
		logstream(LOG_INFO) << "inside resource function"<<std::endl;
	        double cpus =0;
        	double mem = 0;
		double memo = 0;

        	for(size_t i = 0; i< singleSystem.resources_size(); i++)
        	{
            		const Resource resource = singleSystem.resources(i);
		        if ((resource.name() == "cpus" &&
                 	resource.type() == Value::SCALAR)) {
                 		cpus = resource.scalar().value();
		 		logstream(LOG_INFO)<<"Available resources are "<<cpus<<std::endl;
             	}
            	if((resource.name() == "mem" &&
                 resource.type() == Value::SCALAR)) {
                	 mem = resource.scalar().value();
		 	 memo = (mem / 1000);
		 	 logstream(LOG_INFO)<<"Available resources in terms of memory(mb) " <<memo<<std::endl;
             	}
        	}
	logstream(LOG_INFO)<<"Available resources are "<<cpus<<std::endl;
        if(cpus<cpus_graphlab || memo<mem_graphlab)
        {
            driver -> declineOffer(singleSystem.id());
            logstream(LOG_INFO)<<"Available resources are less. The number of deficient cpus are "<<cpus_graphlab-cpus<<" and available memory is "<<mem_graphlab-memo<<std::endl;
            break;
        }
        cpus -= cpus_graphlab;
        memo -= mem_graphlab;

        vector<TaskInfo> tasks;
	int taskId,launchedTasks = 0;
   	     //     for(int i=0; i < cpus_graphlab; i++)
	    logstream(LOG_INFO)<<"Before while loop"<<std::endl;
		int j = 0;
	    graphlab::sched_status::status_enum next_task;
	    //next_task = edited->scheduler_ptr->get_next( i,ret_vid);//edit
	    //while(1)
            
	    //	j++;
            //graphlab::sched_status::status_enum next_task;
            //taskId = launchedTasks++;
            //next_task = graphlab::sched_status::status_enum get_next(cpuid, ret_vid);
            for(size_t k = 0; k < 10000; ++k)
            {
	    //next_task = edited->scheduler_ptr->get_next( k % cpus_graphlab,ret_vid);
	    //edited->thrgroup.launch(boost::bind(edited->thread_start, edited, k));
	    //next_task = edited->scheduler_ptr->get_next(i,ret_vid);
             //logstream(LOG_INFO)<<"Before if loop"<<std::endl;
	    //if(next_task == graphlab::sched_status::NEW_TASK)
	    //do
	    edited->threading(k,edited);
	    if(!edited->endgame_mode)
	    {
	    taskId = launchedTasks++;
            TaskInfo taskNext;
            taskNext.set_name("Graphlab Tasks");
	    std:: string c = "Graphlab Task is  " + taskId;
	    //edited->thrgroup.launch(boost::bind(&refer::thread_start, edited, k));
	    //edited->threading(k,edited);
	    //edited->thread_start(k);
	    graph_type::lvid_type ret_vid;
            taskNext.mutable_task_id() -> set_value(lexical_cast<string>(k));
            taskNext.mutable_slave_id() -> MergeFrom(singleSystem.slave_id());
	    //logstream(LOG_INFO)<<"Inside if loop"<<std::endl;
	    //next_task = edited->scheduler_ptr->get_next( i,ret_vid);
            Resource* resource;
//setting up cpu resources
			  resource = taskNext.add_resources();
            resource->set_name("cpus");
            resource->set_type(Value::SCALAR);
            resource->mutable_scalar()->set_value(cpus_task);
//setting up memory resources
			 resource = taskNext.add_resources();
            resource->set_name("mem");
            resource->set_type(Value::SCALAR);
            resource->mutable_scalar()->set_value(mem_graphlab);
//          tasks.push_back(taskNext);
//          Check mesos.protos. TaskInfo also requires executor iinformation
//	    ExecutorInfo executorInfo;
	    //logstream(LOG_INFO)<<"Name of task "<<taskNext.name()<<std::endl;
	    taskNext.mutable_executor()->MergeFrom(executorInfo);

            Resource* resources;
            resources = executorInfo.add_resources();
            resource->set_name("cpus");
            resource->set_type(Value::SCALAR);
            resource->mutable_scalar()->set_value(cpus_graphlab);

            resources = executorInfo.add_resources();
            resource->set_name("mem");
            resource->set_type(Value::SCALAR);
            resource->mutable_scalar()->set_value(mem_graphlab);
//Now that all the task information is popped up, we are submitting the task to task queue
//logstream(LOG_INFO)<<"Before break statement"<<std::endl;
 tasks.push_back(taskNext);
//driver->launchTasks(singleSystem.id(),tasks);
//break;
}
        //edited->thrgroup.join();    
	//while(next_task != graphlab::sched_status::EMPTY);

//break;	
//	break;
}
	//edited->thrgroup.join();
	logstream(LOG_INFO)<<".............................................................This is ultimate"<<std::endl;
        driver->launchTasks(singleSystem.id(),tasks);
    }
    }
/*
void setMemory(size_t memory)
	{
		mem_graphlab = memory;
	}
size_t retMemory()
	{
		return mem_graphlab;
	}
void setCpus(size_t cpus)
	{
		cpus_graphlab = cpus;
	}
size_t retCpus()
	{
		return cpus_graphlab;
	}
void calculation(size_t memory, size_t cpus) { 
	mem_graphlab = memory;
	setCpus(cpus);
	setMemory(memory);
	cpus_graphlab = cpus; 
	logstream(LOG_INFO)<<"Required cpus are "<<cpus_graphlab<<std::endl;
    array_cpu = new int[cpus_graphlab];

    for(int i=0; i < cpus_graphlab; i++)
        array_cpu[i] = i;

    logstream(LOG_INFO) << "Total memory allocated " <<mem_graphlab<<std::endl;
       }*/
};
#endif
