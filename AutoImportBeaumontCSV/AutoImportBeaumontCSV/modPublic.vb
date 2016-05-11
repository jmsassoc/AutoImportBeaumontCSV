Imports System.Configuration
Imports System.IO
Imports System.Data.SqlClient

Module modPublic
    Public strErrMod As String = ""
    Public conn1 As SqlConnection

    '获取及连接字典库
    Public Function connSqlDb() As Boolean
        Dim blnCnnOk As Boolean
        Try
            conn1 = New SqlConnection(ConfigurationManager.ConnectionStrings("con1").ConnectionString)
            conn1.Open()
            blnCnnOk = True
        Catch ex As Exception
            blnCnnOk = False
        End Try
        Return blnCnnOk
    End Function


    '建立日志
    Public Sub CreatLog(ByVal strFileName As String, ByVal strLog As String)
        Dim strLogFolder As String
        strLogFolder = Application.StartupPath & "\Log\"
        If Not My.Computer.FileSystem.DirectoryExists(strLogFolder) Then
            My.Computer.FileSystem.CreateDirectory(strLogFolder)
        End If
        Dim sw As StreamWriter = New StreamWriter(strLogFolder & strFileName, True) 'true是指以追加的方式打开指
        sw.WriteLine(strLog & vbTab & Now)
        sw.Flush()
        sw.Close()
        sw = Nothing
    End Sub
End Module
