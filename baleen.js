"use strict";

var keratin = require('keratin');
var _ = require('underscorem');

var parsicle = require('parsicle')

var reservedTypeNames = ['type', '^', 'collection','stream','snapshot'];
exports.reservedTypeNames = reservedTypeNames;

function applyPrimitive(t, v){
	if(t === 'int'){
		v.int();
	}else if(t === 'string'){
		v.string();
	}else if(t === 'timestamp'){
		v.long();
	}else if(t === 'boolean'){
		v.boolean();
	}else if(t === 'byte'){
		v.byte();
	}else if(t === 'long'){
		v.long();
	}else if(t === 'binary'){
	//	console.log('applied binary')
		v.binary();
	}else if(t === 'primitive'){
		var e = v.either();
			e.long();
			e.string();
			e.boolean();
	}else{
		_.errout('TODO: ' + JSON.stringify(t));
	}
}
function make(schemaStr, wrapped, useCodes){
	var schema = keratin.parse(schemaStr, reservedTypeNames);
	makeFromSchema(schema, wrapped, useCodes);
}
function applyInclude(schema, name, v, useCodes, inline, isView){
	_.assertString(name);
	
	//console.log('isView: ' + name + ' ' + isView + ' ' + useCodes)
	if(name.indexOf('^') === 0){
		var count = 0;
		for(var i=0;i<name.length;++i){
			var c = name.charAt(i)
			if(c !== '^') break;
			++count;
		}
		if(name.length > count){
			var name = name.substr(count);
			var sch = schema;
			for(var i=0;i<count && sch;++i){
				sch = sch.wrappedSchema;
			}
			if(sch === undefined) _.errout('cannot find wrapped schema up ' + count + ' (failed at ' + i + ')');
			if(sch[name] === undefined) _.errout('cannot find name (' + name + ') in wrapped schema: ' + JSON.stringify(sch));
			var typeCode = sch[name].code;
			_.assertInt(typeCode);
			v.wrapped(count, ''+typeCode);
		}else{
			v.wrapped(count);
		}
	}else{
		if(inline){
			if(useCodes){
				var objSchema = schema[name];
				if(objSchema === undefined) _.errout('unknown type included: ' + name);
				v.include(objSchema.code);
			}else{
				v.include(name);
			}
		}else{
			var e = v.either()
			if(useCodes){
				var objSchema = schema[name];
				if(objSchema === undefined) _.errout('unknown type included: ' + name);
				e.include(objSchema.code);
			}else{
				e.include(name);
			}
			if(isView){
				e.string();
			}
			e.int();
		}
	}
	//console.log('after end: ' + JSON.stringify(v));
}
function makeFromSchema(schema, wrapped, useCodes, includeMeta){	
	var wrappedPs = wrapped ? wrapped._ps : undefined;
	
	if(wrapped) schema.wrappedSchema = wrapped._schema;
	
	//console.log('schema: ' + JSON.stringify(schema).slice(0,2500))
	
	var ps = parsicle.make(wrappedPs, function(parser){
		_.each(schema._byCode, function(objSchema){
			
			var isView = objSchema.isView;
			//console.log('isView ' + objSchema.name + ' ' + isView);
			
			var id = useCodes ? objSchema.code : objSchema.name;
			
			parser(id, 'object', function(ps){
				//var cur = ps;
				if(includeMeta){
					//console.log('including meta')
					var meta = ps.key('meta').object();
					meta.key('typeCode').int();
					
					var idKey = meta.key('id');
					if(isView){
						idKey.string()
					}else{
						idKey.int()
					}
					
					meta.key('editId').int();
				}
				
				_.each(objSchema.properties, function(p){
					var v;
					if(useCodes){
						if(p.tags.required){
							v = ps.key(p.code);
						}else{
							v = ps.optionalKey(p.code);
						}
					}else{
						if(p.tags.required){
							v = ps.key(p.name);
						}else{
							v = ps.optionalKey(p.name);
						}
					}
					var type = p.type.type;
					if(type === 'primitive'){
						applyPrimitive(p.type.primitive, v);
					}else if(type === 'list'){
						var arr = v.array();
						var loop = arr.loop();
						if(p.type.members.type === 'primitive'){
							applyPrimitive(p.type.members.primitive, loop);
						}else if(p.type.members.type === 'object'){
							applyInclude(schema, p.type.members.object, loop, useCodes, p.tags.inline, isView);
						}else{
							_.errout('TODO: ' + JSON.stringify(p));
						}
					}else if(type === 'object'){
						var object = p.type.object;
						applyInclude(schema, object, v, useCodes, p.tags.inline, isView);
					}else if(type === 'map'){
						var b = v.object();
						var nv = b.rest();
						//console.log('map: ' + JSON.stringify(p));
						if(p.type.value.type === 'primitive'){
							applyPrimitive(p.type.value.primitive, nv);
						}else if(p.type.value.type === 'object'){
							applyInclude(schema, p.type.value.object, nv, useCodes,p.tags.inline, isView);
						}else{
							_.errout('TODO: ' + JSON.stringify(p));
						}
					}else if(type === 'set'){
						var arr = v.array();
						var loop = arr.loop();
						if(p.type.members.type === 'primitive'){
							applyPrimitive(p.type.members.primitive, loop);
						}else if(p.type.members.type === 'object'){
							applyInclude(schema, p.type.members.object, loop, useCodes,p.tags.inline, isView);
						}else{
							_.errout('TODO: ' + JSON.stringify(p));
						}
					}else{
						_.errout('TODO: ' + JSON.stringify(p));
					}
				})	
			})
			
		})
	})
	
	function makeBinaryStreamWriter(bufferSize, ws){
		if(arguments.length === 1){
			_.assertFunction(bufferSize.write)
			_.assertFunction(bufferSize.end)
			return ps.binary.stream.makeWriter(bufferSize);
		}
		_.assertFunction(ws.write)
		_.assertFunction(ws.end)
		return ps.binary.stream.makeWriter(bufferSize, ws);
	}
	function makeBinaryStreamReader(readers){
		return ps.binary.stream.makeReader(readers);
	}

	function makeBinarySingleWriter(cb){
		return ps.binary.single.makeWriter(cb);
	}
	function makeBinarySingleReader(readers){
		return ps.binary.single.makeReader(readers);
	}
		
	var h = {
		binary: {
			stream: {
				makeWriter: makeBinaryStreamWriter,
				makeReader: makeBinaryStreamReader
			},
			single: {
				makeWriter: makeBinarySingleWriter,
				makeReader: makeBinarySingleReader
			}
		},
		ascii: {
		},
		_ps: ps,
		_schema: schema
	};
	return h;
}

exports.make = make;
exports.makeFromSchema = makeFromSchema;
