import { aggCategory, pgCategoryCode } from '../Common/PgInfo';

const getDataTypeInfo = (row, categoryInfo, colId) => {
  const columnTypeValueMap = {};
  // Splits comma seperated type names
  const typInfo = row[0].split(',');
  // Splits comma seperated type display names
  const typDisplayName = row[1].split(',');
  // Splits comma seperated type descriptions
  const typDescription = row[2].split(':');
  // Create option object for every valid type
  const currTypeObj = typInfo.map((t, i) => {
    const optObj = {
      value: t,
      label: typDisplayName[i],
      key: `${categoryInfo}_${i}`,
      colIdentifier: colId,
      description: typDescription[i],
    };
    // Memoizing option for later use
    columnTypeValueMap[t] = optObj;
    return optObj;
  });
  return { typInfo: currTypeObj, typValueMap: columnTypeValueMap };
};

/*
 * Input arguments:
 *  dataTypes -> Frequently used types
 *  , restTypes -> Information queried from database
 *  , identifier -> Identifies where this column to be tracked
 * Output:
 *  1) Type -> grouped option
 *  2) returns array of `grouped` options
 * */
const getDataOptions = (dataTypes, restTypes, identifier) => {
  let columnTypeValueMap = {};
  const columnDataTypes = [];
  const mainOpts = [];
  dataTypes.forEach((d, dKey) => {
    mainOpts.push({
      value: d.value,
      label: d.name,
      description: d.description,
      key: dKey,
      colIdentifier: identifier,
    });
    columnTypeValueMap[d.value] = mainOpts[mainOpts.length - 1];
  });
  columnDataTypes.push({
    label: 'Frequently Used Types',
    options: mainOpts,
  });

  /*
   * restTypes will be a list of arrays,
   * each array will have
   * [
   *  "Types available in a particular group",
   *  "Display Name of a type",
   *  "Description of a type",
   *  "Category the particular type belongs to"
   * ]
   * */
  aggCategory.forEach(category => {
    const categoryRow = restTypes.filter(r => r[3] === category);
    if (categoryRow.length > 0) {
      const { typInfo, typValueMap } = getDataTypeInfo(
        categoryRow[0],
        pgCategoryCode[category],
        identifier
      );
      columnTypeValueMap = { ...columnTypeValueMap, ...typValueMap };
      columnDataTypes.push({
        label: pgCategoryCode[category],
        options: typInfo,
      });
    }
  });
  return {
    columnTypeValueMap,
    columnDataTypes,
  };
};

const getPlaceholder = column => {
  switch (column.type) {
    case 'timestamptz':
      return 'example: now()';
    case 'date':
      return '';
    case 'uuid':
      return 'example: gen_random_uuid()';
    default:
      return 'default_value';
  }
};

const getDefaultValue = column => {
  return ('default' in column && column.default.value) || '';
};

export { getDataOptions, getPlaceholder, getDefaultValue };
