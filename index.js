/** @module jsonapi-knex */
var pluralize = require('pluralize')

/**
 * jsonapi-express operations for knex
 * This will fill in all the required operations, and return an object ready for decoration.
 * You may add a transforms dictionary and/or an authorize middleware function to this object before passing to jsonapi-express.
 *
 * @param  {knex} db          A preconfigured knex instance
 * @param  {object} schemas   A dictionary of schemas
 * @param  {object} tables    A dictionary of name:table mappings, for specifying what a schemas' table is named. If a schema is unspecified, the table name will be assumed to match the schema name.
 * @return {object}           An operations dictionary to pass to jsonapi-express
 */
module.exports = function(db, schemas, tables) {
  var getTable = tableMap(tables)
  var operations = {}
  var addIncludes = includesFunction(db, getTable, schemas)
  function find(type, fields, filter, opts, buildQuery) {
    var qb = query(db, schemas, getTable)(type, fields, filter)
    if (typeof buildQuery === 'function') buildQuery(qb)
    return qb
  }
  operations.findAll = function(type, fields, filter, opts, buildQuery) {
    var filterInfo = parseFilter(filter)
    var qb = find.call(this, type, fields, filterInfo.filter, opts, buildQuery)
    return addIncludes(qb, type, filterInfo.includeModels)
  }
  operations.findOne = function(type, fields, filter, opts, buildQuery) {
    var filterInfo = parseFilter(filter)
    var qb = find.call(this, type, fields, filterInfo.filter, opts, buildQuery).first()
    return addIncludes(qb, type, filterInfo.includeModels)
  }
  operations.create = function(type, data) {
    type = getTable(type)
    return db(type)
      .insert(getAttributes(data))
  }
  operations.update = function(type, id, data) {
    type = getTable(type)
    var id = parseInt(id, 10)
    return db(type)
      .where('id', id)
      .update(getAttributes(data))
      .then(() => {
        // TODO: should we support includes on update? JSONAPI spec is unclear.
        return query(db, schemas, getTable)(type, '*', { params: { id: id } }).first()
      })
  }
  operations.delete = function(type, id) {
    type = getTable(type)
    return db(type)
      .where('id', parseInt(id, 10))
      .delete()
  }
  operations.updateRelationship = function(relationship, record, data) {
    throw new Error('unsupported')
  }
  return operations
}

function tableMap(tables) {
  tables = tables || {}
  return function(type) {
    return tables[type] || type
  }
}

function parseFilter(filter) {
  if (filter.query) {
    var include = filter.query.include
    if (include) {
      var includeModels = include.split(',').map(s => s.trim())
      delete filter.query.include
    }
  }
  return { filter, includeModels }
}

// Check if a lookup is empty. Validates array and primitive types
function hasUndefined(lookup) {
  if (typeof lookup === 'undefined' || lookup === null) {
    return true
  } if (Array.isArray(lookup)) {
    return lookup.some(hasUndefined)
  }
}

function getIncludeQuery(qb, name, type, res, relationship) {
  // TODO: support deep includes (type will be dot-separated)
  var foreignKey = relationship.foreignKey
  var idKey = relationship.idKey || 'id'
  if (relationship.relationship === 'belongsTo') {
    foreignKey = foreignKey || `${name}_id`
    var lookup = Array.isArray(res) ? res.map(r => r[foreignKey]) : res[foreignKey]
    if (hasUndefined(lookup)) throw new Error(`Asked to include ${name} with a ${type} query, but ${type} did not have a ${foreignKey}.`)
    qb.whereIn(idKey, lookup)
  } else if (relationship.relationship === 'hasMany') {
    foreignKey = foreignKey || `${pluralize(type, 1)}_id`
    var lookup = Array.isArray(res) ? res.map(r => r[idKey]) : res[idKey]
    if (hasUndefined(lookup)) throw new Error(`Asked to include ${name} with a ${type} query, but ${type} did not have a ${idKey}.`)
    qb.whereIn(foreignKey, lookup)
  } else {
    throw new Error(`Unsupported relationship type ${relationship.relationship}`)
  }
  return qb.select('*')
}

function includesFunction(db, getTable, schemas) {
  return function addIncludes(qb, type, includeModels) {
    return qb.then(res => {
      if (includeModels && includeModels.length && res) {
        var includedTypes = []
        return Promise.all(includeModels.map(modelName => {
          var relationship = schemas[type] && schemas[type][modelName]
          if (!relationship) throw new Error(`Asked for related ${modelName} for ${type}, but the relationship does not exist.`)
          includedTypes.push(relationship.type)
          var qb = db(getTable(relationship.type))
          return getIncludeQuery(qb, modelName, type, res, relationship)
        })).then(included => {
          res = { data: res }
          res.included = includedTypes.reduce((o, k, idx) => {
            o[k] = included[idx]
            return o
          }, {})
          return res
        })
      }
      return { data: res }
    })
  }
}

function query(db, schemas, getTable){
  return function(type, fields, filter) {
    type = getTable(type)
    var qb = db(type)
    if (filter.join) {
      qb
        .select(filter.join.fields)
        .leftOuterJoin(filter.join.table, filter.join.left, filter.join.right)
    } else if (fields) {
      qb.select(fields)
    }
    if (filter.params) qb.where(filter.params)
    if (filter.query) qb.where(filter.query)
    return qb
  }
}

function getAttributes(data) {
  var relationships = data.data.relationships
  var attributes = data.data.attributes
  if (relationships) {
    Object.keys(relationships)
      .forEach(r => {
        attributes[`${r}_id`] = relationships[r].data
                                ? parseInt(relationships[r].data.id, 10)
                                : relationships[r].data
      })
  }
  return attributes
}
