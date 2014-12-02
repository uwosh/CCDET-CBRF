import csv
import logging
import transaction

DIRNAME = "/opt/Plone-4.3/zeocluster/Extensions/"

FILENAME = "ccdet.dat"

MAXROWS = 100000000
TRANSSIZE = 50
TRANSSIZE_FOR_READING = 5000
FOLDERID = 'cbrf-folder'
logger = logging.getLogger('ccdet_mem')

html_escape_table = {
    "&": "&amp;",
    }

def html_escape(text):
    """Produce entities within text."""
    return "".join(html_escape_table.get(c,c) for c in text)

def ccdet_mem(self, readonly):
    allrecs = {}
    readonly = (readonly == 1 or readonly == True)

    def ccdet_read(self):
        transcount = 0
        totalcount = 0
        with open('%s/%s' % (DIRNAME, FILENAME), 'rb') as csvfile:
            linereader = csv.DictReader(csvfile, dialect=csv.excel_tab)

            current_person_id = ''
            current_person_object = None
            last_person_object = None
            last_unique_key = ''

            for row in linereader:
                (
                    unique_key,
                    last_name,
                    first_name,
                    middle_initial,
                    last_4,
                    birthdate,
                    phone,
                    email,
                    employment_status,
                    class_name,
                    start_date,
                    city,
                    state,
                    trainer_name,
                    trainer_approval_number
                    ) = (
                    row['P_Unique_Key'],
                    row['Last_Name'],
                    row['First_Name'],
                    row['Middle_Initial'],
                    row['Last_4'],
                    row['P_Birthdate'],
                    row['P_Phone'],
                    row['Email'],
                    row['Employment_Status'],
                    row['Class_Name'],
                    row['Start_Date'],
                    row['C_City'],
                    row['C_State'],
                    row['P_Trainer'],
                    row['P_Trainer_Approval_#']
                )
                # strip out time from start_date
                start_date = start_date.split(' ')[0]
                if employment_status == 'S' and start_date != '':
                    current_person_id_base = '%s-%s-%s-%s' % (last_name, first_name, middle_initial, unique_key)
                    current_person_id_base = current_person_id_base.translate(None, "':\"/&")
                    current_person_id = current_person_id_base
                    if unique_key != last_unique_key:
                        # we are dealing with a new person so close out the previous person record
                        if last_unique_key != '':
                            # do not do this if we are just starting
                            #last_person_object.setText(last_person_object.getText() + '</table>')
                            pass
                        # check if need to create a new person object or can edit an existing person object
                        if not allrecs.has_key(current_person_id):
                            allrecs[current_person_id] = {}
                            #logger.info('created new person id %s' % current_person_id)
                            current_person_object = allrecs[current_person_id]
                            current_person_object['title'] = '%s, %s %s %s' % (last_name, first_name, middle_initial, unique_key)
                            current_person_object['certs'] = []
                        else:
                            #logger.info('skipping existing person ID %s' % current_person_id)
                            current_person_object = allrecs[current_person_id]
                    else:
                        # we are adding a cert to an existing person object
                        current_person_object = allrecs[current_person_id]

                    transcount += 1
                    totalcount += 1
                    current_person_object['certs'].append({'class_name':class_name, 'start_date':start_date, 'city':city, 'state':state, 'trainer_name':trainer_name, 'trainer_approval_number':trainer_approval_number})
                    last_person_object = current_person_object
                    last_unique_key = unique_key
                    transcount += 1
                    if transcount == TRANSSIZE_FOR_READING:
                        transaction.commit()
                        transcount = 0
                        logger.info("read extract line %s" % totalcount)
                    if totalcount == MAXROWS:
                        #logger.error('reached max number of rows to process; bailing now')
                        message = 'reached max number of rows to process (%s); bailing now' % MAXROWS
                        logger.warn(message)
                        return message

    def ccdet_write(self, readonly):
        transcount = 0
        totalcount = 0
        updatecount = 0
        newcount = 0
        identicalcount = 0
        if self.checkIdAvailable(FOLDERID):
            if readonly:
                logger.info('would create folder %s' % id)
            else:
                self.invokeFactory(type_name='Folder', id=FOLDERID)
        folder = getattr(self, FOLDERID, None)
        for current_person_id in allrecs.keys():
            current_person = allrecs[current_person_id]
            if not folder:
                # must be in readonly mode
                logger.info('would create person %s' % current_person_id)
                newcount += 1
            else:
                if folder.checkIdAvailable(current_person_id):
                    if readonly:
                        logger.info('would create person %s' % current_person_id)
                    else:
                        folder.invokeFactory(type_name='CBRFPersonSimple', id=current_person_id)
                        #logger.info('created new person id %s' % current_person_id)
                current_person_object = getattr(folder, current_person_id, None)
                if not readonly:
                    current_person_object.setTitle(current_person['title'])
                    #current_person_object.reindexObject(idxs=["Title"]) # will reindex below
                new_text = '<table width="100%"> <tr> <th> Class Name </th> <th> Date </th> <th> City </th> <th> Trainer </th> <th> Trainer # </th> </tr>\n'
                for cert in current_person['certs']:
                    new_text += '    <tr> <td>%s</td> <td>%s</td> <td>%s %s</td> <td>%s</td> <td>%s</td> </tr>\n' % (cert['class_name'], cert['start_date'], cert['city'], cert['state'], cert['trainer_name'], cert['trainer_approval_number'])
                new_text += '</table>'
                if current_person_object:
                    current_text = current_person_object.getText()
                else:
                    # we are in readonly mode and this would be a new person
                    current_text = ''
                if current_text != html_escape(new_text):
                    if len(current_text) < 10: # arbitrary number
                        logger.info('setting %s' % current_person_id)
                        newcount += 1
                    else:
                        logger.info('updating %s' % current_person_id)
                        #logger.info('OLD: %s\nNEW: %s' % (current_text, html_escape(new_text)))
                        updatecount += 1
                    if not readonly:
                        current_person_object.setText(new_text)
                        current_person_object.reindexObject()
                else:
                    #logger.info('no change to %s' % current_person_id)
                    identicalcount += 1
                transcount += 1
                totalcount += 1
                if transcount == TRANSSIZE:
                    transaction.commit()
                    transcount = 0
                    logger.info("committed transaction %s" % totalcount)
        logger.info('Created %s records. Updated %s records. %s records were unchanged.' % (newcount, updatecount, identicalcount))

    ccdet_read(self)
    if readonly:
        logger.info('running in readonly mode')
    ccdet_write(self, readonly)
    return "Done."
