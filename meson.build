project('easyzmq',
	'c',
	default_options : ['c_std=c11', 'warning_level=2'],
	version : '0.2.0',
	license : 'BSD',
	meson_version : '>= 0.56.0',
)

zmqdep = dependency('libzmq', version : '>= 4.0')
glibdep = dependency('glib-2.0', version : '>= 2.44')
giodep = dependency('gio-2.0', version : '>= 2.44')

inc = include_directories('.')

easyzmq_sources = ['easyzmq.c', 'easyzmqasync.c', 'easyzmqtask.c']
easyzmq_headers = ['easyzmq.h']

easyzmq = library('easyzmq',
	easyzmq_sources,
	include_directories : inc,
	dependencies : [zmqdep, glibdep, giodep],
	version : meson.project_version(),
	soversion : '0',
	install : true)

# to use easyzmq as meson subproject:
#   dependency('easyzmq', fallback : ['easyzmq', 'easyzmq_dep'])
easyzmq_dep = declare_dependency(
	link_with : easyzmq,
	include_directories : inc,
	dependencies : [glibdep, giodep, zmqdep])
meson.override_dependency('easyzmq', easyzmq_dep)

install_headers(easyzmq_headers)

# vala binding: vapi + deps file
vapidir = get_option('datadir') / 'vala' / 'vapi'
install_data('easyzmq.vapi', install_dir : vapidir)
configure_file(output : 'easyzmq.deps',
	input : 'easyzmq.deps.in',
	copy : true,
	install : true,
	install_dir : vapidir)

pkg = import('pkgconfig')
pkg.generate(easyzmq,
	name : 'easyzmq',
	description : 'ZeroMQ sockets integrated in the GLib main loop',
	requires : [glibdep, giodep],
	requires_private : [zmqdep])

# tests (inproc only, no network)
if get_option('tests')
	testauto = executable('testauto',
		['testauto.c'],
		dependencies : [easyzmq_dep])
	test('easyzmq', testauto, timeout : 60)
endif

# examples
if get_option('examples')
	c_examples = {
		'testzmq' : 'test.c',
		'testzmqpub' : 'testpub.c',
		'testzmqpubmon' : 'testpubmon.c',
		'testzmqsub' : 'testsub.c',
		'testzmqresp' : 'testresp.c',
		'testzmqrouterresp' : 'testrouter.c',
		'testzmqreq' : 'testreq.c',
		'testzmqreqtimeout' : 'testreqtimeout.c',
		'testzmqpair1' : 'testpair1.c',
		'testzmqpair2' : 'testpair2.c',
	}
	foreach name, src : c_examples
		executable(name, src, dependencies : [easyzmq_dep])
	endforeach

	if add_languages('vala', required : get_option('examples_vala'), native : false)
		vala_examples = {
			'testzmqreqvala' : 'testreqvala.vala',
			'testzmqreqmonitorvala' : 'testreqmonitorvala.vala',
			'testzmqreqasyncvala' : 'testreqasyncvala.vala',
			'testzmqrepvala' : 'testrepvala.vala',
			'testzmqsubvala' : 'testsubvala.vala',
			'testzmqrepasyncvala' : 'testrepasyncvala.vala',
			'testzmqsubasyncvala' : 'testsubasyncvala.vala',
		}
		# valac would look for a libzmq.vapi: the zmq dependency is only for the C compiler
		easyzmq_vala_dep = declare_dependency(
			link_with : easyzmq,
			include_directories : inc,
			compile_args : ['-I' + zmqdep.get_variable(pkgconfig : 'includedir')])
		foreach name, src : vala_examples
			executable(name, [src, 'easyzmq.vapi'],
				dependencies : [glibdep, giodep, easyzmq_vala_dep],
				vala_args : ['--vapidir', meson.current_source_dir()],
				c_args : ['-w'])
		endforeach

		# self contained async example (inproc) also used as test
		testasyncvala = executable('testzmqasyncvala', ['testasyncvala.vala', 'easyzmq.vapi'],
			dependencies : [glibdep, giodep, easyzmq_vala_dep],
			vala_args : ['--vapidir', meson.current_source_dir()],
			c_args : ['-w'])
		if get_option('tests')
			test('easyzmq-vala-async', testasyncvala, timeout : 60)
		endif
	endif
endif
