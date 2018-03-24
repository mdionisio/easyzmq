project('easyzmq', 'c')
zmqdep = dependency('libzmq')
glibdep = dependency('glib-2.0')

inc = include_directories('.')

easyzmq = library('easyzmq', 
	['easyzmq.c'], 
	include_directories : inc,
	dependencies : [zmqdep, glibdep])

executable('testzmq', 
	['test.c'], 
	include_directories : inc,
	dependencies : [zmqdep, glibdep],
	link_with : [easyzmq])

executable('testzmqpub', 
	['testpub.c'], 
	include_directories : inc,
	dependencies : [zmqdep, glibdep],
	link_with : [easyzmq])

executable('testzmqsub', 
	['testsub.c'], 
	include_directories : inc,
	dependencies : [zmqdep, glibdep],
	link_with : [easyzmq])