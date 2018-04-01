project('easyzmq', 'c', 'vala')
zmqdep = dependency('libzmq')
glibdep = dependency('glib-2.0')

inc = include_directories('.')

add_project_arguments(['--vapidir', meson.current_source_dir()],
                      language: 'vala')


easyzmq = library('easyzmq', 
	['easyzmq.c'], 
	include_directories : inc,
	dependencies : [zmqdep, glibdep])

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

executable('testzmqreq', 
	['testreq.c'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])
	
executable('testzmqreqvala', 
	['testreqvala.vala', 'easyzmq.vapi'], 
	include_directories : inc,
	dependencies : [glibdep],
	link_with : [easyzmq])