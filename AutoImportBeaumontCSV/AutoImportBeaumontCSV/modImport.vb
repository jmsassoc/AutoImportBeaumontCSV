Imports System.Configuration
Imports System.IO
Imports System.Data.SqlClient

Module modImport

    Dim colNewFileList As Collection
    Dim strTableName As String = "TblEligibility"
    Public Sub startImport()
        Dim blnIsBackup As Boolean
        Dim strInfo As String
        Dim strSourceFile As String = ""
        Dim strImportCSVSourceFileFolder As String = ""
        Dim strImportCSVSourceFileMoveFolder As String = ""
        Dim blnOK As Boolean, blnIsBackupTable As Boolean
        blnIsBackupTable = False
        colNewFileList = New Collection
        strImportCSVSourceFileFolder = ConfigurationManager.AppSettings("ImportCSVSourceFileFolder").ToString
        strImportCSVSourceFileMoveFolder = ConfigurationManager.AppSettings("ImportCSVSourceFileMoveFolder").ToString
        If My.Computer.FileSystem.DirectoryExists(strImportCSVSourceFileFolder) Then
            strInfo = CheckGetNewFile(strImportCSVSourceFileFolder)
            If strInfo <> "" Then
                Call CreatLog("LoadFileErr.log", strInfo)
            ElseIf colNewFileList.Count >= 1 Then

                blnIsBackupTable = False
                For i = 1 To colNewFileList.Count
                    strSourceFile = colNewFileList.Item(i)
                    blnOK = False
                    'Try
                    If Not blnIsBackupTable Then
                        '先进行表的备份
                        blnIsBackup = BackupTable()
                        blnIsBackupTable = True
                    End If
                    If blnIsBackup Then
                        blnOK = LoadAndImportCSVLine(strSourceFile)
                    Else
                        Call CreatLog("SysErrLog.log", "Backup table error!")
                        Exit For
                    End If

                    '设置文件导完时间及移除文件
                    If blnOK Then
                        Call MoveSourceFile(strSourceFile, strImportCSVSourceFileMoveFolder)
                    End If
                    'Catch ex As Exception
                    '    Call CreatLog("SysErrLog.log", strSourceFile & " CSV source file Error!" & ex.Message)
                    'End Try
                Next
            End If
        Else
            Call CreatLog("SysErrLog.log", strImportCSVSourceFileFolder & " CSV source file directory not found!")
        End If
    End Sub


    '加载及导入数据
    Public Function LoadAndImportCSVLine(ByVal strSourceFile As String) As Boolean

        Dim strError As String = ""
        Dim FileName As String = "'"
        Dim lngUniqueEmpID As Long = 0
        Dim EmployeeID As String, FirstName As String, LastName As String, Birthdate As String, Suffix As String, SSN As String, Location As String, UniqueEmpID As String
        Dim rsCSV As ADODB.Recordset
        Dim myTrans As SqlTransaction
        Dim blnImport As Boolean = False
        Dim sqlAdaRule As New SqlDataAdapter("SELECT *  FROM    " & strTableName & " Order by UniqueEmpID Desc ", conn1)
        Dim cmdBuilderRule As New SqlCommandBuilder(sqlAdaRule)
        Dim rsTblEmployees As DataTable = New DataTable(strTableName)
        Dim blnTrans As Boolean = False

        Dim sqlAdaRuleFind As New SqlDataAdapter("SELECT max(UniqueEmpID) as UniqueEmpID, LastName, Birthdate,SSN  FROM    " & strTableName & " group by LastName, Birthdate,SSN  ", conn1)
        Dim cmdBuilderRuleFind As New SqlCommandBuilder(sqlAdaRule)
        Dim rsTblEmployeesClone As DataTable = New DataTable(strTableName)
        Dim numOfRows As Long
        Dim objKey() As Object
        sqlAdaRule.Fill(rsTblEmployees)
        sqlAdaRuleFind.Fill(rsTblEmployeesClone)
        rsTblEmployees.PrimaryKey = New DataColumn() {rsTblEmployees.Columns("Location"), rsTblEmployees.Columns("EmployeeID")}

        rsTblEmployeesClone.PrimaryKey = New DataColumn() {rsTblEmployeesClone.Columns("LastName"), rsTblEmployeesClone.Columns("Birthdate"), rsTblEmployeesClone.Columns("SSN")}
        Dim strFilter As String
        Dim rsRuleRows() As DataRow
        Dim rsRow As DataRow
        Dim rsRowLine As DataRow
        Dim strNow As String, strNowTime As String
        strNowTime = Now
        strNow = Now.ToString("yyyy-MM-dd")

        rsCSV = LoadCSVFile(strSourceFile)
        '开始比较及导入
        rsCSV.Filter = 0
        If rsCSV.RecordCount >= 1 Then
            FileName = rsCSV.Fields("FileName").Value & ""
            Location = rsCSV.Fields("Location").Value & ""
            myTrans = conn1.BeginTransaction
            sqlAdaRule.SelectCommand.CommandTimeout = 1200
            sqlAdaRule.SelectCommand.Transaction = myTrans

        Else
            FileName = ""
            Location = ""
        End If
        If rsTblEmployees.Rows.Count <= 0 Then
            lngUniqueEmpID = 0
        Else
            lngUniqueEmpID = CLng(rsTblEmployees.Rows(0).Item("UniqueEmpID").ToString)
        End If
        Do While Not rsCSV.EOF
            EmployeeID = rsCSV.Fields("EmployeeID").Value & ""
            LastName = rsCSV.Fields("LastName").Value & ""
            FirstName = rsCSV.Fields("FirstName").Value & ""
            Birthdate = rsCSV.Fields("Birthdate").Value & ""
            Suffix = rsCSV.Fields("Suffix").Value & ""
            SSN = rsCSV.Fields("SSN").Value & ""
            '先获取 UniqueEmpID  
            objKey = New Object() {LastName, Birthdate, SSN}
            rsRowLine = rsTblEmployeesClone.Rows.Find(objKey)
            If Not IsNothing(rsRowLine) Then
                UniqueEmpID = rsRowLine.Item("UniqueEmpID").ToString()
            Else
                lngUniqueEmpID = lngUniqueEmpID + 1
                UniqueEmpID = Format(lngUniqueEmpID, "00000000")
                rsRow = rsTblEmployeesClone.NewRow()
                rsRow("LastName") = LastName
                ' rsRow("FirstName") = FirstName
                rsRow("Birthdate") = Birthdate
                rsRow("SSN") = SSN
                rsRow("UniqueEmpID") = UniqueEmpID
                rsTblEmployeesClone.Rows.Add(rsRow)
            End If
            objKey = New Object() {Location, EmployeeID}
            rsRowLine = rsTblEmployees.Rows.Find(objKey)
            If IsNothing(rsRowLine) Then '是否增加
                rsRow = rsTblEmployees.NewRow()
                rsRow("EmployeeID") = EmployeeID
                rsRow("LastName") = LastName
                rsRow("FirstName") = FirstName
                rsRow("Birthdate") = Birthdate
                rsRow("Suffix") = Suffix
                rsRow("SSN") = SSN
                rsRow("Location") = Location
                rsRow("UniqueEmpID") = UniqueEmpID
                rsRow("ImportFileName") = FileName & "|" & strNowTime
                rsRow("StatusDate") = strNow
                rsRow("Status") = "A"
                rsTblEmployees.Rows.Add(rsRow)
            Else
                rsRowLine("EmployeeID") = EmployeeID
                rsRowLine("LastName") = LastName
                rsRowLine("FirstName") = FirstName
                rsRowLine("Birthdate") = Birthdate
                rsRowLine("Suffix") = Suffix
                rsRowLine("SSN") = SSN
                rsRowLine("Location") = Location
                rsRowLine("UniqueEmpID") = UniqueEmpID
                rsRowLine("ImportFileName") = FileName & "|" & strNowTime
                rsRowLine("StatusDate") = strNow
                rsRowLine("Status") = "A"
                numOfRows = sqlAdaRule.Update(rsTblEmployees)
            End If
            rsCSV.MoveNext()
        Loop
        If FileName.Length >= 1 Then
            strFilter = "Location='" & Replace(Location, "'", "''") & "' And ImportFileName<>'" & Replace(FileName & "|" & strNowTime, "'", "''") & "'"
            rsRuleRows = rsTblEmployees.Select(strFilter)
            For Each row In rsRuleRows
                EmployeeID = row.Item("EmployeeID").ToString()
                rsCSV.Filter = "EmployeeID='" & Replace(EmployeeID, "'", "''") & "'"
                If rsCSV.EOF Then
                    row("Status") = "D"
                End If
            Next

            Try
                sqlAdaRule.Update(rsTblEmployees)
                myTrans.Commit()
                blnImport = True
            Catch ex As Exception
                myTrans.Rollback()
                blnImport = False
            End Try

        End If
        rsTblEmployees.Dispose()
        cmdBuilderRule.Dispose()
        sqlAdaRule.Dispose()
        Return blnImport
    End Function

    'Public Sub LoadAndImportCSVLine(ByVal strSourceFile As String)
    '    Dim strError As String = ""
    '    Dim FileName As String = "'"
    '    Dim EmployeeID As String, FirstName As String, LastName As String, Birthdate As String, Suffix As String, SSN As String, Location As String, UniqueEmpID As String
    '    Dim rsCSV As ADODB.Recordset
    '    Dim sqlAdaRule As New SqlDataAdapter("SELECT *  FROM    TblEmployeesTest ", conn1)
    '    Dim cmdBuilderRule As New SqlCommandBuilder(sqlAdaRule)
    '    Dim rsTblEmployees As DataTable = New DataTable("TblEmployeesTest")
    '    sqlAdaRule.Fill(rsTblEmployees)
    '    Dim strFilter As String
    '    Dim rsRuleRows() As DataRow
    '    Dim rsRow As DataRow
    '    rsCSV = LoadCSVFile(strSourceFile)
    '    '开始比较及导入
    '    rsCSV.Filter = 0
    '    If rsCSV.RecordCount >= 1 Then
    '        FileName = rsCSV.Fields("FileName").Value & ""
    '        Location = rsCSV.Fields("Location").Value & ""
    '    Else
    '        FileName = ""
    '        Location = ""
    '    End If
    '    Do While Not rsCSV.EOF
    '        EmployeeID = rsCSV.Fields("EmployeeID").Value & ""
    '        LastName = rsCSV.Fields("LastName").Value & ""
    '        FirstName = rsCSV.Fields("FirstName").Value & ""
    '        Birthdate = rsCSV.Fields("Birthdate").Value & ""
    '        Suffix = rsCSV.Fields("Suffix").Value & ""
    '        SSN = rsCSV.Fields("SSN").Value & ""
    '        '先获取 UniqueEmpID  
    '        strFilter = " LastName='" & Replace(LastName, "'", "''") & "' And FirstName='" & Replace(FirstName, "'", "''") & "' " _
    '                & " And Birthdate='" & Replace(Birthdate, "'", "''") & "' And SSN='" & Replace(SSN, "'", "''") & "' "
    '        rsRuleRows = rsTblEmployees.Select(strFilter)
    '        If rsRuleRows.Length >= 1 Then
    '            UniqueEmpID = rsRuleRows(0).Item("UniqueEmpID").ToString()
    '        Else
    '            UniqueEmpID = Rnd8()
    '            strFilter = "UniqueEmpID='" & UniqueEmpID & "'"
    '            rsRuleRows = rsTblEmployees.Select(strFilter)
    '            Do While rsRuleRows.Length >= 1
    '                UniqueEmpID = Rnd8()
    '                strFilter = "UniqueEmpID='" & UniqueEmpID & "'"
    '                rsRuleRows = rsTblEmployees.Select(strFilter)
    '            Loop
    '        End If
    '        strFilter = "Location='" & Replace(Location, "'", "''") & "' And EmployeeID='" & Replace(EmployeeID, "'", "''") & "'"
    '        rsRuleRows = rsTblEmployees.Select(strFilter)
    '        If rsRuleRows.Length <= 0 Then '是否增加
    '            rsRow = rsTblEmployees.NewRow()
    '        Else
    '            rsRow = rsRuleRows(0)
    '        End If
    '        rsRow("EmployeeID") = EmployeeID
    '        rsRow("LastName") = LastName
    '        rsRow("FirstName") = FirstName
    '        rsRow("Birthdate") = Birthdate
    '        rsRow("Suffix") = Suffix
    '        rsRow("SSN") = SSN
    '        rsRow("Location") = Location
    '        rsRow("UniqueEmpID") = UniqueEmpID
    '        rsRow("ImportFileName") = FileName
    '        rsRow("StatusDate") = Now
    '        rsRow("Status") = "A"
    '        If rsRuleRows.Length <= 0 Then '是否增加
    '            rsTblEmployees.Rows.Add(rsRow)
    '        End If
    '        rsCSV.MoveNext()
    '    Loop
    '    If FileName.Length >= 1 Then
    '        strFilter = "Location='" & Replace(Location, "'", "''") & "'"
    '        rsRuleRows = rsTblEmployees.Select(strFilter)
    '        For Each row In rsRuleRows
    '            EmployeeID = row.Item("EmployeeID").ToString()
    '            rsCSV.Filter = "EmployeeID='" & Replace(EmployeeID, "'", "''") & "'"
    '            If rsCSV.EOF Then
    '                row("Status") = "D"
    '            End If
    '        Next

    '        sqlAdaRule.Update(rsTblEmployees)
    '    End If
    '    rsTblEmployees.Dispose()
    '    cmdBuilderRule.Dispose()
    '    sqlAdaRule.Dispose()
    'End Sub
    Private Function Rnd8() As String
        Dim i As Integer
        Dim j As Int16
        Dim strValue As String
        strValue = ""
        For j = 1 To 8
            Randomize()
            i = CInt(Int((10 * Rnd()) + 0))
            strValue = strValue + i.ToString
        Next
        Return strValue
    End Function

    '加载CSV文件
    Public Function LoadCSVFile(ByVal strSourceFile As String) As ADODB.Recordset
        Dim arrSplitLine() As String, arrLine() As String
        Dim strLine As String
        Dim strNewLine As String = ""
        Dim intStartLine As Integer = -1
        Dim lngLine As Long = -1
        Dim FileIndex As Long = 0
        Dim blnIsUpdate As Boolean = False
        Dim strError As String = ""
        Dim strFileBaseName As String
        Dim strLocationName As String
        Dim rsCSV As ADODB.Recordset
        Dim file As FileInfo = New FileInfo(strSourceFile)
        Dim strSplitString As String = ""
        strFileBaseName = file.Name.Substring(0, file.Name.Length - IIf(file.Extension.Length = 0, 0, file.Extension.Length + 0))

        rsCSV = CreateSourceCSVRs()
        If Left(UCase(strFileBaseName), Len("Beaumont")) = UCase("Beaumont") Then
            strLocationName = "BHS" ' Left(strFileBaseName, Len("Beaumont"))
            intStartLine = 1
            strSplitString = ","
        ElseIf Left(UCase(strFileBaseName), Len("Oakwood")) = UCase("Oakwood") Then
            intStartLine = 1
            strLocationName = Left(strFileBaseName, Len("Oakwood"))
            strSplitString = ","
        ElseIf Left(UCase(strFileBaseName), Len("botsford")) = UCase("botsford") Then
            intStartLine = 1
            strLocationName = Left(strFileBaseName, Len("botsford"))
            strSplitString = ","
        Else
            intStartLine = -1
            strSplitString = ","
        End If

        If intStartLine >= 0 Then '符合文件的格式才导入
            lngLine = 0
            FileIndex = 0
            Dim sr As New System.IO.StreamReader(strSourceFile)
            Do Until sr.EndOfStream = True
                strLine = sr.ReadLine
                lngLine = lngLine + 1
                If lngLine >= intStartLine Then
                    FileIndex = FileIndex + 1
                    'arrSplitLine = Split(strLine, strSplitString)

                    arrLine = Split(strLine, Chr(34))
                    strNewLine = ""
                    For i = 1 To arrLine.Length
                        If i Mod 2 = 0 Then
                            strNewLine = strNewLine & Replace(arrLine(i - 1), strSplitString, vbCrLf)
                        Else
                            strNewLine = strNewLine & arrLine(i - 1)
                        End If
                    Next
                    ' strNewLine = Replace(arrLine(j), vbCrLf, ",")
                    arrSplitLine = Split(strNewLine, strSplitString)
                    rsCSV.AddNew()
                    rsCSV.Fields("EmployeeID").Value = ReplaceVbCrLf(arrSplitLine(0), strSplitString)
                    rsCSV.Fields("LastName").Value = ReplaceVbCrLf(arrSplitLine(1), strSplitString)
                    rsCSV.Fields("FirstName").Value = ReplaceVbCrLf(arrSplitLine(2), strSplitString)
                    rsCSV.Fields("Birthdate").Value = ReplaceVbCrLf(arrSplitLine(4), strSplitString)
                    rsCSV.Fields("Suffix").Value = ReplaceVbCrLf(arrSplitLine(3), strSplitString)
                    rsCSV.Fields("SSN").Value = Left(Trim(ReplaceVbCrLf(arrSplitLine(5), strSplitString)), 4)
                    rsCSV.Fields("Location").Value = strLocationName
                    'rsCSV.Fields("UniqueEmpID").Value = ""
                    rsCSV.Fields("FileName").Value = file.Name
                    rsCSV.Fields("FileIndex").Value = FileIndex
                    rsCSV.Update()
                End If
            Loop
            sr.Close()
            sr = Nothing
        End If
        Return rsCSV
    End Function
    Private Function ReplaceVbCrLf(ByVal strValue As String, ByVal strSplitString As String) As String
        ReplaceVbCrLf = Replace(strValue, vbCrLf, strSplitString)
    End Function

    '构建一个记录集(比例值)
    Public Function CreateSourceCSVRs() As ADODB.Recordset
        Dim rs As ADODB.Recordset
        rs = New ADODB.Recordset

        rs.Fields.Append("EmployeeID", ADODB.DataTypeEnum.adVarChar, 30)
        rs.Fields.Append("FirstName", ADODB.DataTypeEnum.adVarChar, 50)
        rs.Fields.Append("LastName", ADODB.DataTypeEnum.adVarChar, 50)
        rs.Fields.Append("Birthdate", ADODB.DataTypeEnum.adVarChar, 10)
        rs.Fields.Append("Suffix", ADODB.DataTypeEnum.adVarChar, 20)
        rs.Fields.Append("SSN", ADODB.DataTypeEnum.adVarChar, 4)
        rs.Fields.Append("Location", ADODB.DataTypeEnum.adVarChar, 20)
        rs.Fields.Append("UniqueEmpID", ADODB.DataTypeEnum.adVarChar, 8)

        rs.Fields.Append("FileName", ADODB.DataTypeEnum.adVarChar, 250)
        rs.Fields.Append("FileIndex", ADODB.DataTypeEnum.adInteger)

        rs.Open()

        Return rs
    End Function


    '检验是否有新的文件
    Private Function CheckGetNewFile(ByVal strImportCSVPath As String) As String
        Dim fileList() As String
        Dim rsID As SqlDataReader
        Dim i As Integer = 0
        Dim strSql As String
        Dim CommandSql As SqlCommand
        Dim strFileCreateTime As String
        Dim strFileSize As String
        Dim strSourceFileFolder As String
        Try
            colNewFileList = New Collection
            '获取当前目录下所有文件信息
            strSourceFileFolder = strImportCSVPath
            fileList = System.IO.Directory.GetFileSystemEntries(strSourceFileFolder)
            CommandSql = New SqlCommand()
            CommandSql.CommandTimeout = 600
            CommandSql.Connection = conn1
            For i = 0 To fileList.Length - 1
                If IO.File.Exists(fileList(i)) Then
                    Dim file As FileInfo = New FileInfo(fileList(i))
                    strFileCreateTime = file.CreationTime
                    strFileSize = file.Length
                    '检验文件是否已OK
                    strSql = "SELECT  * FROM TblUpdateFileFlag  WHERE (FileName = '" & Replace(fileList(i), "'", "''") & "') "
                    CommandSql.CommandText = strSql
                    rsID = CommandSql.ExecuteReader()
                    If rsID.Read() Then
                        If (Trim(rsID.Item("CreateTime").ToString()) = strFileCreateTime) And (Trim(rsID.Item("FileSize").ToString()) = strFileSize) Then
                            colNewFileList.Add(fileList(i))
                        End If
                        rsID.Close()
                        strSql = "UPDATE  TblUpdateFileFlag SET FileSize ='" & strFileSize & " ', CreateTime ='" & strFileCreateTime & "'" _
                            & " WHERE (FileName = '" & Replace(fileList(i), "'", "''") & "') "
                        CommandSql.CommandText = strSql
                        CommandSql.ExecuteNonQuery()
                    Else
                        rsID.Close()
                        strSql = "INSERT INTO TblUpdateFileFlag (FileName, FileSize, CreateTime)" _
                            & " VALUES('" & Replace(fileList(i), "'", "''") & "','" & strFileSize & "' ,'" & strFileCreateTime & "' )"
                        CommandSql.CommandText = strSql
                        CommandSql.ExecuteNonQuery()
                    End If
                End If
            Next
            Return ""
        Catch ex As Exception
            Return ex.Message
        End Try
    End Function

    Public Function BackupTable() As Boolean
        Dim strBackupTableName As String
        Dim CommandSql As SqlCommand
        Dim strSql As String
        CommandSql = New SqlCommand()
        CommandSql.CommandTimeout = 600
        Try
            CommandSql.Connection = conn1
            strBackupTableName = strTableName & "_BAK" & Format(Now, "yyyyMMddhhmmss")
            strSql = "SELECT   * INTO " & strBackupTableName & "  FROM      " & strTableName
            CommandSql.CommandText = strSql
            Call CommandSql.ExecuteNonQuery()
            CommandSql.Dispose()
            Return True
        Catch ex As Exception
            Return False
        End Try

    End Function
    Public Sub MoveSourceFile(ByVal strImportCSVSourceFile As String, ByVal strImportCSVSourceFileMoveFolder As String)
        Dim strMoveFileFolder As String
        Dim strFile As String
        Dim strSql As String
        Dim CommandSql As SqlCommand
        Try
            strFile = strImportCSVSourceFile
            strMoveFileFolder = strImportCSVSourceFileMoveFolder
            If Not My.Computer.FileSystem.FileExists(strMoveFileFolder) Then
                My.Computer.FileSystem.CreateDirectory(strMoveFileFolder)
            End If
            If IO.File.Exists(strFile) Then
                Dim file As FileInfo = New FileInfo(strFile)
                If IO.File.Exists(strMoveFileFolder & "\" & file.Name) Then
                    Rename(strMoveFileFolder & "\" & file.Name, strMoveFileFolder & "\M" & Format(Now, "Hmmss") & file.Name)
                End If
                file.MoveTo(strMoveFileFolder & "\" & file.Name)
                CommandSql = New SqlCommand()
                CommandSql.CommandTimeout = 600
                CommandSql.Connection = conn1
                strSql = "UPDATE  TOP (1) TblUpdateFileFlag SET DoneTime = getdate()  Where FileName='" & Replace(strImportCSVSourceFile, "'", "''") & "'"
                CommandSql.CommandText = strSql
                Call CommandSql.ExecuteNonQuery()
                CommandSql.Dispose()
            End If
        Catch ex As Exception
            Call CreatLog("SysErrLog.log", strImportCSVSourceFile & " MoveSourceFile Error!" & ex.Message)
        End Try
    End Sub

End Module
