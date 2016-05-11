Public Class FrmMain

    Private Sub FrmMain_Load(sender As System.Object, e As System.EventArgs) Handles MyBase.Load
        Dim blnCnnOk As Boolean
        ' Exit Sub
        Try
            strErrMod = "[FrmMain_Load]"
            '只允许一个实例运行()
            If PrevInstance(IO.Path.GetFileNameWithoutExtension(Application.ExecutablePath), Application.ExecutablePath, True) Then
                ' MsgBox(Application.ExecutablePath)
                Me.Dispose()
                Exit Sub
            End If

            Me.Left = -10000
            Me.WindowState = False
            Me.ShowInTaskbar = False
            Me.Hide()
            blnCnnOk = connSqlDb()
            If blnCnnOk Then
                Call startImport()
            Else
                Call CreatLog("SysErrLog.log", "Dictionary database connection error")
            End If
            Me.Dispose()
            End
        Catch ex As Exception
            Call CreatLog("SystemErr.log", ex.Message & strErrMod)
            End
        End Try
    End Sub
    Private Function PrevInstance(ByVal sProName As String, ByVal strApplicPath As String, Optional ByVal start As Boolean = False) As Boolean
        Dim i As Integer, j As Integer
        '位置
        Dim strProcessPath As String
        Dim processes() As Process
        i = 0
        Try
            processes = Process.GetProcesses()
            For j = 0 To processes.GetLength(0)
                'If UCase(processes(j).ProcessName) = UCase(sProName) Then
                strProcessPath = GetProcessesPath(processes(j))
                If UCase(strProcessPath) = UCase(strApplicPath) Then
                    i = i + 1
                    If i >= 2 Then
                        Exit For

                    End If
                End If
            Next
        Catch ex As Exception
            i = 0
        End Try
        If start Then
            Return i > 1
        Else
            Return i > 0
        End If
    End Function

    Private Function GetProcessesPath(ByVal obj1 As Object) As String
        Dim strPath As String
        strPath = ""
        Try
            strPath = obj1.MainModule.FileName
        Catch ex As Exception

        End Try
        Return strPath
    End Function

    Private Sub Button1_Click(sender As System.Object, e As System.EventArgs) Handles Button1.Click
        Dim blnCnnOk As Boolean
        blnCnnOk = connSqlDb()
        If blnCnnOk Then
            Debug.Print(Now)
            Call startImport()
            Debug.Print(Now)
        Else
            Call CreatLog("SysErrLog.log", "Dictionary database connection error")
        End If
    End Sub
End Class
