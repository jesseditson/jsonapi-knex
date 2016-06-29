/**
 * jsonapi-knex
 * jsonapi-express operations for knex
 * This will fill in all the required operations, and return an object ready for decoration.
 * You may add a sideEffects dictionary and/or an authorize middleware function to this object before passing to jsonapi-express.
 *
 * @param  {knex} db          A preconfigured knex instance
 * @param  {object} schemas   A dictionary of schemas
 * @return {object}           An operations dictionary to pass to jsonapi-express
 */
module.exports = function(db, schemas) {
  var operations = {}
  operations.findAll = function(type, fields, filter) {
    return query(db, schemas)(type, fields, filter)
  }
  operations.findOne = function(type, fields, filter) {
    return query(db, schemas)(type, fields, filter).first()
  }
  operations.create = function(type, data) {
    return db(type)
      .insert(getAttributes(data))
  }
  operations.update = function(type, id, data) {
    var id = parseInt(id, 10)
    return db(type)
      .where('id', id)
      .update(getAttributes(data))
      .then(() => {
        return query(db, schemas)(type, '*', { params: { id: id } }).first()
      })
  }
  operations.delete = function(type, id) {
    return db(type)
      .where('id', parseInt(id, 10))
      .delete()
  }
  operations.updateRelationship = function(relationship, record, data) {
    throw new Error('unsupported')
  }
  return operations
}

/**
 * Private
 */

function query(db, schemas){
  return function(type, fields, filter) {
    var qb = db(type)
    if (filter.join) {
      qb
        .select(filter.join.fields)
        .leftOuterJoin(filter.join.table, filter.join.left, filter.join.right)
    } else {
      qb.select(fields)
    }
    if (filter.params) qb.where(filter.params)
    if (filter.query) {
      // var include = filter.query.include
      // if (include) {
      //   var includeModels = include.split(',').map(s => s.trim())
      //   // TODO: support deep includes
      //   delete filter.query.include
      // }
      qb.where(filter.query)
    }
    return qb
    // TODO: something like this to fetch relationships. Need to pull them off the schema to find the query.
    // .then(res => {
    //   if (includeModels && res.length) {
    //     return Promise.all(includeModels.map(modelName => {
    //       db(modelName)
    //         .select('*')
    //         .whereIn({
    //           `${type}_id`: res.map(r => r.id)
    //         })
    //         .orWhereIn({
    //           id: res.map(r => r[])
    //         })
    //     }))
    //   }
    //   return res
    // })
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
