import os
import logging
import sys
import datetime
import shutil
import json
from concurrent.futures import ThreadPoolExecutor
import subprocess

def main():

    # Einrichtung der Protokolldatei
    logging.basicConfig(filename='TerminalServer.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
    logging.info("\n\n" + "******************************************************************************************")
    logging.info("Starting " + sys.argv[0])

    now = datetime.datetime.now()

    jsonFile = 'updateTerminalServer.json'

    if os.path.isfile(jsonFile):
        with open(jsonFile, 'r') as file:
            try:
                data = json.load(file)
            except:
                print("Can't read updateTerminalServer.json!")
                logging.info("Can't read updateTerminalServer.json!")
                progQuit()
    else:
        print("Can't find updateTerminalServer.json!")
        logging.info("Can't find updateTerminalServer.json!")
        progQuit()

    try:
        tServer = data["serverPaths"]
    except:
        print("Can't find serverPaths-keyword in updateTerminalServer.json!")
        logging.info("Can't find serverPaths-keyword in updateTerminalServer.json!")
        progQuit()

    tServerContent = []
    tServerSwitch  = []

    try:
        workDir = data["workDir"]
    except:
        print("Can't find workDir-keyword in updateTerminalServer.json!")
        logging.info("Can't find workDir-keyword in updateTerminalServer.json!")
        progQuit()

    updateDir = os.path.join(workDir, str(now.year) + "-xx-xx")

    # Kontrolle, ob die beiden Verzeichnisse vorhanden sind, falls nicht wird abgebrochen
    checkDir(workDir)
    checkDir(updateDir)

    # Kontrolle ob das Verzeichnis mit den neuen Dateien leer ist
    # Wenn nicht leer werden alle Dateinamen in einer Liste gespeichert
    if not os.listdir(updateDir):
        print("Nothing to do! Directory is empty: " + updateDir)
        logging.info(updateDir + " is empty")
        progQuit()
    else:
        updateDirContent = [os.path.join(root, file) for root, dirnames, filenames in os.walk(updateDir) for file in filenames]
        updateDirContent = list(map(lambda x: x.replace(updateDir + "\\", ""), updateDirContent))
        for line in updateDirContent:
            logging.info("Found in " + updateDir + ":  " + line)

    # Überprüfe, ob alle Pfade der Server vorhanden sind
    # Alle Schalterzustände werden aufgenommen
    # Alle Ordnerinhalte je Server werden ausgelesen und aufgenommen
    for server in tServer:
        checkDir(server)
        tServerSwitch.append(checkSwitch(server))
        tmp = [os.path.join(root, file) for root, dirnames, filenames in os.walk(server) for file in filenames]
        tServerContent.append(list(map(lambda x: x.replace(server + "\\", ""), tmp)))

    # Überprüfe, ob alle Schalter der Serverpfade gleich sind
    if not checkSwitchEqual(tServerSwitch):
        print("\nNot all switches are set the same:")
        logging.info("Not all switches are set the same:")
        for i in range(0, len(tServer)):
            print(tServer[i], tServerSwitch[i])
            logging.info(tServer[i] + " " + tServerSwitch[i])
        print()
        progQuit()

    # Erstellung einer Liste mit Pfaden
    # Die Pfade enthalten die Quellen und Ziele für den Kopiervorgang
    copyAssignment = []
    for item in updateDirContent:
        baseItem = os.path.basename(item)
        count, index = checkAvailability(baseItem, tServerContent[0])

        # Es wird geprüft, ob die Zieldateien vorhanden sind
        # Es wird ebenfalls registriert, wenn die Zieldateien mehrfach vorhanden sind
        # Bsp.-Ordner: RES1 und RES2
        # Wenn nicht vorhanden, dann wird nachgefragt, welcher Ordner genutzt werden soll
        if index != []:
            logString = index[0]
            copyAssignment.append([item, index[0]])
            for i in range(1, len(index)):
                logString = logString + ", " + index[i]
                copyAssignment.append([item, index[i]])
            logging.info(item + " found " + str(count) + " time(s): " + logString)
        else:
            print(item + " was not found on server!")
            logging.info(item + " was not found on server!")
            newFileOnServer = howProceed(item)
            copyAssignment.append([item, newFileOnServer])
            logging.info("New destination for " + item + " is " + newFileOnServer)

    # Erstellung eines Backups des Auslegungsprogramms je Server
    # folder_BCK1 -> folder_BCK2 und folder -> folder_BCK1
    # 190318 CK: Für jeden Server wird ein eigener Thread gestartet, um die Ausführungszeit zu minimieren
    logging.info("")
    with ThreadPoolExecutor(max_workers=len(tServer)) as executor:
        for server in tServer:
            executor.submit(makeBackup, (server))
    logging.info("")

    # Eigentlicher Kopiervorgang der Dateien auf allen Servern
    flagRES = []
    for i in range(0, len(tServer)):
        server = tServer[i]
        print("Updating Server " + server)
        logging.info("Updating Server " + server)
        flagThermofinExe = 0
        flagKalkTesterExe = 0
        flagRES.append(0)

        for item in copyAssignment:

            # Es gerpüft, ob die Thermofin.exe ein Update erhält
            # Dies wird nur einmal je Server durchgeführt
            # Die alte Thermofin.exe wird mit old deklariert
            if item[1] == "Thermofin.exe":
                if flagThermofinExe == 0:
                    removeOldThermofinExe(server)
                    src = os.path.join(updateDir, item[0])
                    dst = os.path.join(server, item[1])
                    #shutil.copyfile(src, dst) # 190625 Datum der Dateien wird sonst mitgeändert
                    shutil.copy2(src, dst)
                    logging.info("Update " + item[1])
                    flagThermofinExe = 1
                else:
                    print("Can't update Thermofin.exe with " + item[0])
                    print("It's already updated ...")
                continue

            # Es gerpüft, ob die KalkTester.exe ein Update erhält
            # Dies wird nur einmal je Server durchgeführt
            # Die alte KalkTester.exe wird mit old deklariert
            if item[1] == "Kalk\\KalkTester.exe":
                if flagKalkTesterExe == 0:
                    removeOldKalkTesterExe(server)
                    src = os.path.join(updateDir, item[0])
                    dst = os.path.join(server, item[1])
                    shutil.copy2(src, dst)
                    logging.info("Update " + item[1])
                    flagKalkTesterExe = 1
                else:
                    print("Can't update KalkTester.exe with " + item[0])
                    print("It's already updated ...")
                continue

            # Hier erhalten die Access-Datenbanken ein Update
            # Dies geschieht abhängig vom Schalterzustand
            # Dabei werden zunächst die Datenbanken aus dem aktiven Ordner in den inaktiven Ordner kopiert
            if item[1].find("RES") != -1:
                newSwitch, currentSwitch = getSwitch(tServerSwitch[i])
                if flagRES[i] == 0:
                    src = os.path.join(server, currentSwitch)
                    dst = os.path.join(server, newSwitch)
                    try:
                        shutil.rmtree(dst)
                        logging.info("Remove content from " + dst)
                    except:
                        print("Can't remove content from " + dst)
                        logging.info("Can't remove content from " + dst)
                        progQuit()
                    logging.info("Copy databases from " + currentSwitch + " to " + newSwitch)
                    shutil.copytree(src, dst)
                    flagRES[i] = 1

                # Anschließend werden die neuen Datenbanken in den inaktiven (neuen) Ordner kopiert
                if item[1].find(newSwitch) != -1:
                    src = os.path.join(updateDir, item[0])
                    dst = os.path.join(server, item[1])
                    #shutil.copyfile(src, dst) # 190625 Datum der Dateien wird sonst mitgeändert
                    shutil.copy2(src, dst)
                    logging.info("Update " + item[1])

                continue

            # Es werden alle restlichen Dateien auf die Server kopiert
            # Falls ein neuer Ordner erstellt werden muss, wird dies getan
            src = os.path.join(updateDir, item[0])
            dst = os.path.join(server, item[1])
            dstPath = os.path.dirname(dst)
            if not os.path.isdir(dstPath):
                os.makedirs(dstPath)
                logging.info("Create new directory: " + dstPath)
            try:
                #shutil.copyfile(src, dst) # 190625 Datum der Dateien wird sonst mitgeändert
                shutil.copy2(src, dst)
                logging.info("Update " + item[1])
            except:
                print("Can't update " + dst)
                logging.info("Can't update " + dst)

    # Falls eine Datenbank kopiert wurde, wird der Schalter für RES umgeschaltet
    # Dazu wird die entsprechende Thermofin.ini kopiert und der Ordner "RES1/2 ist aktiv" umbenannt
    for i in range(0, len(tServer)):
        server = tServer[i]
        if flagRES[i] == 1:
            newSwitch, currentSwitch = getSwitch(tServerSwitch[i])
            try:
                if newSwitch == "RES1":
                    # shutil.copyfile(os.path.join(server, "Thermofin.ini1"), os.path.join(server, "Thermofin.ini")) # 190625 Datum der Dateien wird sonst mitgeändert
                    shutil.copy2(os.path.join(server, "Thermofin.ini1"), os.path.join(server, "Thermofin.ini"))
            except:
                print("Can't copy Thermofin.ini1 to Thermofin.ini on " + server)
                logging.info("Can't copy Thermofin.ini1 to Thermofin.ini on " + server)
                progQuit()

            try:
                if newSwitch == "RES2":
                    # shutil.copyfile(os.path.join(server, "Thermofin.ini2"), os.path.join(server, "Thermofin.ini")) # 190625 Datum der Dateien wird sonst mitgeändert
                    shutil.copy2(os.path.join(server, "Thermofin.ini2"), os.path.join(server, "Thermofin.ini"))
            except:
                print("Can't copy Thermofin.ini1 to Thermofin.ini on " + server)
                logging.info("Can't copy Thermofin.ini1 to Thermofin.ini on " + server)
                progQuit()

            os.rename(os.path.join(server, currentSwitch + " ist aktiv"), os.path.join(server, newSwitch + " ist aktiv"))
            print("Change switch on " + server + " to " + newSwitch)
            logging.info("Change switch on " + server + " to " + newSwitch)


    # der Ordner 20xx-xx-xx wird in ein Datumsformat umbenannt, das dem aktuellen Datum entspricht
    dateString = str(now.year) + "-" + str(now.month).zfill(2) + "-" + str(now.day).zfill(2)
    newDir = os.path.join(workDir, dateString)
    archiveDir = os.path.join(workDir, str(now.year))
    try:
        os.rename(updateDir, newDir)
        logging.info("Rename " + str(now.year) + "-xx-xx to " + newDir)
    except:
        print("Can't rename " + str(now.year) + "-xx-xx directory to " + newDir)
        logging.info("Can't rename " + str(now.year) + "-xx-xx directory to " + newDir)
        progQuit()

    # der Ordner mit dem aktuellen Datum wird in einen Archivordner bewegt
    try:
        if not os.path.isdir(archiveDir):
            try:
                os.mkdir(archiveDir)
                logging.info("Create directory: " + archiveDir)
            except:
                print("Can't create " + archiveDir)
                logging.info("Can't create " + archiveDir)

        # falls es schon einen solchen Ordner im Archiv gibt, wird eine fortlaufende Zahl angefügt
        number = 1
        tmpDir = os.path.basename(newDir)
        baseDir = tmpDir 
        while os.path.isdir(os.path.join(archiveDir, tmpDir)):
            tmpDir = baseDir + "(" + str(number) + ")"
            number = number + 1
        archiveDir = os.path.join(archiveDir, tmpDir)

        shutil.move(newDir, archiveDir)
        logging.info("Move " + newDir + " into " + archiveDir)
    except:
        print("Can't move " + newDir + " into " + archiveDir)
        logging.info("Can't move " + newDir + " into " + archiveDir)

    # Es wird ein neuer leerer Ordner für zukünfitge Updates erstellt
    try:
        os.mkdir(os.path.join(workDir, str(now.year) + "-xx-xx" ))
    except:
        print("Can't create "+ str(now.year) + "-xx-xx directory.")
        logging.info("Can't create "+ str(now.year) + "-xx-xx directory.")

    progQuit()


# Ab hier beginnen die Unterfunktionen:

def checkDir(dir):
    if os.path.isdir(dir):
        logging.info("Directory available: " + dir)
    else:
        print("Directory NOT available: " + dir)
        logging.info("Directory NOT available: " + dir)
        progQuit()


def checkSwitch(dir):
    if os.path.isdir(dir + "\\RES1 ist aktiv"):
        logging.info(dir + " RES1 ist aktiv")
        return "RES1"
    if os.path.isdir(dir + "\\RES2 ist aktiv"):
        logging.info(dir + " RES2 ist aktiv")
        return "RES2"
    logging.info("No 'RES1/2 ist aktiv' directory in " + dir)
    print("Cannot find RES1/2 directory in " + dir)
    progQuit()


def checkSwitchEqual(lst):
   return lst[1:] == lst[:-1]


def checkAvailability(baseItem, serverContent):
    count = 0
    index = []
    for entry in range(0, len(serverContent)):
        if baseItem in serverContent[entry]:
            count += 1
            index.append(serverContent[entry])
    return count, index


def howProceed(item):
    newPath = os.path.dirname(item)
    response = ""
    while response != "y":
        print("Should file " + item + " be copied into ...\\thermofin SP\\" + newPath + "\\ ? (y/n/e)")
        response = input(":")
        if response == "n":
            print("No permission to change anything. -> Abort")
            progQuit()
        if response == "e":
            print("Enter new path:")
            newPath = input("thermofin SP\\")
        print()
    return os.path.join(newPath, os.path.basename(item))


def makeBackup(server):
    # 190729 CK: Backup-Ordner in ein separates Backupverzeichnis
    # pathBCK1 = server + "_BCK1"
    # pathBCK2 = server + "_BCK2"
    pathTMP = os.path.split(server)
    pathBCK1 = pathTMP[0] + "\\Backup\\" + pathTMP[1] + "_BCK1"
    pathBCK2 = pathTMP[0] + "\\Backup\\" + pathTMP[1] + "_BCK2"

    if os.path.isdir(pathBCK2):
        print("Remove " + pathBCK2)
        logging.info("Remove " + pathBCK2)
        shutil.rmtree(pathBCK2)
    if os.path.isdir(pathBCK1):
        print("Rename " + pathBCK1)
        logging.info("Rename " + pathBCK1)
        shutil.move(pathBCK1, pathBCK2)
    print("Make Backup of " + server)
    logging.info("Make Backup of " + server)
    # 190723 CK: Kopieren der Ordner mit robocopy von Windows, um Attribute wie "versteckt" zu erhalten
    # shutil.copytree(server, pathBCK1)
    subprocess.run( 'robocopy "' + server + '" "' + pathBCK1 + '" /COPY:DAT /E /V /NDL /NFL')# , stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    


def removeOldThermofinExe(server):
    exeName = ["Thermofin(old).exe", "Thermofin (old).exe", "(old)Thermofin.exe", "(old) Thermofin.exe", "(old )Thermofin.exe", "( old)Thermofin.exe"]
    for name in exeName:
        oldExe = os.path.join(server, name)
        if os.path.isfile(oldExe):
            try:
                os.remove(oldExe)
                logging.info("Remove " + name)
            except:
                print("Can't remove " + oldExe)
                print("Maybe it's in use ...")
                logging.info("Can't remove " + oldExe)
                progQuit()

    src = os.path.join(server, "Thermofin.exe")
    if os.path.isfile(src):
        dst = os.path.join(server, exeName[0])
        os.rename(src, dst)


def removeOldKalkTesterExe(server):
    exeName = ["Kalk\\KalkTester(old).exe", "Kalk\\KalkTester (old).exe", "Kalk\\(old)KalkTester.exe", "Kalk\\(old) KalkTester.exe", "Kalk\\(old )KalkTester.exe", "Kalk\\( old)KalkTester.exe"]
    for name in exeName:
        oldExe = os.path.join(server, name)
        if os.path.isfile(oldExe):
            try:
                os.remove(oldExe)
                logging.info("Remove " + name)
            except:
                print("Can't remove " + oldExe)
                print("Maybe it's in use ...")
                logging.info("Can't remove " + oldExe)
                progQuit()

    src = os.path.join(server, "Kalk\\KalkTester.exe")
    if os.path.isfile(src):
        dst = os.path.join(server, exeName[0])
        os.rename(src, dst)


def getSwitch(switch):
    if switch == "RES1":
        return "RES2", "RES1"
    elif switch == "RES2":
        return "RES1", "RES2"

def progQuit():
    progName = sys.argv[0]
    logging.info("Quit " + progName + "\n" + "******************************************************************************************")
    # quit()
    sys.exit(0)


# Starte das Programm mit der main-Funktion
if __name__ == '__main__':
    main()