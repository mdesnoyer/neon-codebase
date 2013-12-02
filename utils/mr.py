'''Map reduce utilities

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

class CompositeMRJob(MRJob):
    '''A map reduce job that is composed of many separate jobs.

    All the subjobs work on the same data stream, but we only ever process
    that data once.

    To use this class, subclass it and instantiate the jobs() function.

    '''
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol

    def __init__(self, args):
        '''Create the job.

        args - command line arguments
        '''
        self.job_mapper = {}
        self.step_mapper = {}
        self.max_steps = 0
        i = 0
        for job in self.jobs():
            self.job_map[i] = job
            self.step_map[i] = job.steps()
            self.max_steps = max(self.max_steps, len(self.step_mapper[i]))
            i += 1

        super(CompositeMRJob, self).__init__(args)

    def jobs(self, jobs):
        '''Defines a list of jobs to perform.

        Must return a list of MRJob objects that will be run by this compound
        job

        '''
        raise NotImplementedError

    def configure_options(self):
        super(CompositeMRJob, self).configure_options()
        for job in job_mapper.itervalues():
            job.add_passthrough_option = self.add_passthrough_option
            job.configure_options()

    def first_mapper(self, key, value):
        '''Mapper that gives the line to each job.'''
        for job_id, steps in self.step_map.iteritems():
            for out_key, out_value in steps[0]['mapper'](key, value)
                yield (job_id, out_key), out_value

    def mapper_wrapper(self, key, value, step=1):
        '''A wrapper around a set of mappers.

        Only works for step >0.

        key - (job_id, mapper_key)
        value - mapper_value
        step - The step to execute
        '''
    
        job_id, mapper_key = key
        try:
            mapper = self.step_map[job_id][step]['mapper']
        except IndexError:
            # There is no step for this job, so just pass the data through
            yield key, value
            return
            
        for out_key, out_value in mapper(mapper_key, value):
            yield (job_id, out_key), out_value

    def comb_red_wrapper(self, key, values, step=0, steptype='reducer'):
        '''A wrapper around combiner and reducer functions.

        key - (job_id, internal_key)
        values - generator of values
        step - step to run for
        steptype - 'reducer' or 'combiner'
        '''
        job_id, internal_key = key
        try:
            func = self.step_map[job_id][step][steptype]
        except IndexError:
            if step == self.max_steps-1 and steptype == 'reducer':
                # The end of the chain
                cur_key = internal_key
            else:
                cur_key = key
            for value in values:
                yield cur_key, values
            return
        if func:
            for out_key, out_value in func(internal_key, values):
                if step == self.max_steps-1 and steptype == 'reducer':
                    cur_key = internal_key
                else
                    cur_key = (job_id, out_key)
                yield cur_key, out_value

    def initfinal_wrapper(self, steptype, step=0):
        '''Wraps a init or final functions.

        steptype - one of "mapper_init", "mapper_final", "combiner_init", etc..

        '''
        for job_id, steps in selfstep_map.iteritems():
            try:
                func = steps[step][steptype]
            except IndexError:
                continue
            
            if func:
                for out_key, out_value in func() or ():
                    yield (job_id, out_key), out_value
        

    def steps(self):
        out_steps = []
        for i in range(self.max_steps):
            # Determine which functions are needed for this step
            do_parts = { 'mapper_init': False,
                         'mapper_final': False,
                         'combiner_init': False,
                         'combiner': False,
                         'combiner_final': False,
                         'reducer_init': False
                         'reducer_final': False }
            for job_steps in self.step_map.values():
                try:
                    cur_step = job_steps[i]
                except IndexError:
                    continue
                for func_name in do_parts.keys():
                    if cur_step[func_name] is not None:
                        do_parts[func_name] = True

            out_steps.append(self.mr(
                mapper_init= (lambda: self.initfinal_wrapper('mapper_init', i)
                              if do_parts['mapper_init'] else None),
                mapper = (self.first_mapper if i == 0 else
                          lambda k, v: self.mapper_wrapper(k, v, i)),
                mapper_final= (lambda: self.initfinal_wrapper('mapper_final', i)
                              if do_parts['mapper_final'] else None),
                combiner_init= (lambda: self.initfinal_wrapper('combiner_init',
                                                               i)
                              if do_parts['combiner_final'] else None),
                combiner = (lambda k, v: self.comb_red_wrapper(k,v,i,'combiner')
                              if do_parts['combiner'] else None),
                combiner_final= (lambda: self.initfinal_wrapper('combiner_final'
                                                                , i)
                              if do_parts['combiner_final'] else None),
                reducer_init= (lambda: self.initfinal_wrapper('reducer_init',
                                                               i)
                              if do_parts['reducer_final'] else None),
                reducer = lambda k, v: self.comb_red_wrapper(k,v,i,'reducer'),
                reducer_final= (lambda: self.initfinal_wrapper('reducer_final'
                                                                , i)
                              if do_parts['reducer_final'] else None)))

    return out_steps
        
