project('easyzmq', 
	'c', 'vala', 
	default_options: ['c_std=c11'],
	version: '0.1',
    license: 'BSD',
)

zmqdep = dependency('libzmq')
glibdep = dependency('glib-2.0')
giodep = dependency('gio-2.0')

inc = include_directories('.')

add_project_arguments(['--vapidir', meson.current_source_dir()],
                      language: 'vala')


easyzmq = library('easyzmq', 
	['easyzmq.c', 'easyzmqasync.c'], 
	include_directories : inc,
	dependencies : [zmqdep, glibdep, giodep])

# to use easyzmq as meson subproject: dependency('easyzmq', fallback: ['easyzmq', 'easyzmq_dep'])
easyzmq_dep = declare_dependency(
	link_with : easyzmq,
	include_directories : inc,
	dependencies : [glibdep, giodep])

testauto = executable('testauto',
	['testauto.c'],
	dependencies : [easyzmq_dep, zmqdep])
test('easyzmq', testauto, timeout : 60)

executable('testzmq', 
	['test.c'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])

executable('testzmqpub', 
	['testpub.c'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])

executable('testzmqpubmon', 
	['testpubmon.c'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])

executable('testzmqsub', 
	['testsub.c'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])

executable('testzmqresp', 
	['testresp.c'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])

executable('testzmqrouterresp', 
	['testrouter.c'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])

executable('testzmqreq', 
	['testreq.c'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])

executable('testzmqreqtimeout', 
	['testreqtimeout.c'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])

executable('testzmqpair1', 
	['testpair1.c'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])

executable('testzmqpair2', 
	['testpair2.c'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])
	
executable('testzmqreqvala', 
	['testreqvala.vala', 'easyzmq.vapi'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])

executable('testzmqreqmonitorvala', 
	['testreqmonitorvala.vala', 'easyzmq.vapi'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])

executable('testzmqreqasyncvala', 
	['testreqasyncvala.vala', 'easyzmq.vapi'], 
	include_directories : inc,
	dependencies : [glibdep, giodep],
	link_with : [easyzmq])

executable('testzmqrepvala', 
	['testrepvala.vala', 'easyzmq.vapi'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])

executable('testzmqsubvala', 
	['testsubvala.vala', 'easyzmq.vapi'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])